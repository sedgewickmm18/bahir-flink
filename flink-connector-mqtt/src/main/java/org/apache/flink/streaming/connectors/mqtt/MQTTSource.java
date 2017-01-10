/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.mqtt;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
//import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
//import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
//import org.apache.flink.streaming.connectors.mqtt.internal.MQTTUtil;
import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;


/**
 * Source for reading messages from an ActiveMQ queue.
 * <p>
 * To create an instance of AMQSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * This source is waiting for incoming messages from ActiveMQ and converts them from
 * an array of bytes into an instance of the output type. If an incoming
 * message is not a message with an array of bytes, this message is ignored
 * and warning message is logged.
 *
 * If checkpointing is enabled AMQSink will not acknowledge received AMQ messages as they arrive,
 * but will store them internally and will acknowledge a bulk of messages during checkpointing.
 *
 * @param <OUT> type of output messages
 */
public class MQTTSource<OUT> extends MessageAcknowledgingSourceBase<OUT, String>
    implements ResultTypeQueryable<OUT>, MqttCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSource.class);

    // starting with mqtt example in m2mIO-gister/SimpleMqttClient.java
    static final String BROKER_URL = "tcp://localhost:1883";
    static final String M2MIO_DOMAIN = "<Insert m2m.io domain here>";
    static final String M2MIO_STUFF = "things";
    static final String M2MIO_THING = "MARKUS JAVA CLIENT";
    static final String M2MIO_USERNAME = "markus";
    static final String M2MIO_PASSWORD_MD5 = "286755fad04869ca523320acce0dc6a4";


    // Factory that is used to create AMQ connection
    //private final ActiveMQConnectionFactory connectionFactory;
    private int QoS;

    // Name of a queue or topic
    private final String topicName;
    // Deserialization scheme that is used to convert bytes to output message
    private final DeserializationSchema<OUT> deserializationSchema;

    // Throw exceptions or just log them
    private boolean logFailuresOnly = false;
    // Stores if source is running (used for testing)
    private RunningChecker runningChecker;
    // MQTT connect options
    private transient MqttConnectOptions connOpts;
    // AMQ session
    private transient MqttClient mqttClient;

    // If source should immediately acknowledge incoming message
    private boolean autoAck;

    private SourceContext<OUT> flinkCtx = null;

    // Map of message ids to currently unacknowledged MQTT messages
    private HashMap<String, MqttMessage> unacknowledgedMessages = new HashMap<>();

    // Listener for AMQ exceptions
    private MQTTExceptionListener exceptionListener;

    /**
     *
     * this callback is invoked upon losing the MQTT connection.
     * @param t
     */
    @Override
    public void connectionLost(Throwable t) {
        System.out.println("Connection lost!");
        // code to reconnect to the broker would go here if desired
    }

    /**
     *
     * deliveryComplete
     * This callback is invoked when a message published by this client
     * is successfully received by the broker.
     *
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        try {
            System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
        }
        catch (MqttException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }



    /**
     *
     * messageArrived
     * This callback is invoked when a message is received on a subscribed topic.
     *
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("-------------------------------------------------");
        System.out.println("| Topic:" + topic);
        System.out.println("| Message: " + new String(message.getPayload()));
        System.out.println("-------------------------------------------------");

        byte[] bytes = message.getPayload();

        if (flinkCtx == null) {return;}

        OUT value = deserializationSchema.deserialize(bytes);

        synchronized (flinkCtx.getCheckpointLock()) {
                flinkCtx.collect(value);
                if (!autoAck) {
                    String messageId = Integer.toString(message.getId());
                    addId(messageId);
                    unacknowledgedMessages.put(messageId, message);
                }
         }
    }

    /**
     * Create MQTTSource.
     *
     * @param config MQTTSource configuration
     */
    public MQTTSource(MQTTSourceConfig<OUT> config) {
        super(String.class);
        //this.connectionFactory = config.getConnectionFactory();
        this.connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(30);

        this.topicName = config != null ? config.getDestinationName() : "hm";
        this.deserializationSchema = config != null ? config.getDeserializationSchema() : null;
        this.runningChecker = config != null ? config.getRunningChecker() : null;
    }

    /**
     * Defines whether the producer should fail on errors, or only log them.
     * If this is set to true, then exceptions will be only logged, if set to false,
     * exceptions will be eventually thrown and cause the streaming program to
     * fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    // Visible for testing
    void setExceptionListener(MQTTExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);

        String clientID = M2MIO_THING;

        // set user credentials
        connOpts.setUserName(M2MIO_USERNAME);
        connOpts.setPassword(M2MIO_PASSWORD_MD5.toCharArray());

        // Create a Connection
        try {
            mqttClient = new MqttClient(BROKER_URL, clientID);
            mqttClient.setCallback(this);
            mqttClient.setManualAcks(true);
            mqttClient.connect(connOpts);
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        //exceptionListener = new AMQExceptionListener(LOG, logFailuresOnly);
        //connection.setExceptionListener(exceptionListener);

        RuntimeContext runtimeContext = getRuntimeContext();
        if (runtimeContext instanceof StreamingRuntimeContext
            && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
            autoAck = false;
            //acknowledgeType = ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;
            QoS = 2;
        } else {
            autoAck = true;
            //acknowledgeType = ActiveMQSession.AUTO_ACKNOWLEDGE;
            QoS = 1;
        }

        runningChecker.setIsRunning(true);
    }

    @Override
    public void close() throws Exception {
        super.close();
        RuntimeException exception = null;
        try {
            mqttClient.close();
        } catch (MqttException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close MQTT session", e);
            } else {
                exception = new RuntimeException("Failed to close MQTT consumer", e);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void acknowledgeIDs(long checkpointId, List<String> UIds) {
        try {
            for (String messageId : UIds) {
                MqttMessage unacknowledgedMessage = unacknowledgedMessages.get(messageId);
                if (unacknowledgedMessage != null) {
                    mqttClient.messageArrivedComplete(unacknowledgedMessage.getId(), QoS);
                    //unacknowledgedMessage.acknowledge();
                    unacknowledgedMessages.remove(messageId);
                } else {
                    LOG.warn("Tried to acknowledge unknown MQTT message id: {}", messageId);
                }
            }
        } catch (MqttException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to acknowledge MQTT message");
            } else {
                throw new RuntimeException("Failed to acknowledge MQTT message");
            }
        }
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        flinkCtx = ctx;
        System.out.println("subscribing to " + topicName);
        //exceptionListener.checkErroneous();


        try {
            //mqttClient.subscribe(topicName, QoS);
            autoAck = false;
            mqttClient.subscribe(topicName, 2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        runningChecker.setIsRunning(false);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

/* to test locally in the IDE's debugger
    public static void main(String[] args) {
        MQTTSource<String> smc = new MQTTSource<String>(null);
        try {
            smc.open(null);

        }
        catch (Exception e) {
            //
        }
        try {
            smc.run(null);

        }
        catch (Exception e) {
            //
        }
    }
*/

}
