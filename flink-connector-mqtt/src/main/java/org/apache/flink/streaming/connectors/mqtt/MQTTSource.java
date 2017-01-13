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

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
//import org.apache.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
//import org.apache.flink.streaming.connectors.mqtt.internal.MQTTUtil;
import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import javax.net.ssl.SSLContext;


/**
 * Source for reading messages from an MQTT queue.
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

    private ArrayBlockingQueue<MqttMessage> blockingQueue;

    // Factory that is used to create AMQ connection
    //private final ActiveMQConnectionFactory connectionFactory;
    private int QoS;

    // URL of the broker
    private final String brokerURL;

    // MQTT user name/ APIKey
    private final String userName;

    // MQTT password / APISecret
    private final String password;

    // MQTT clientId
    private final String clientId;

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
    private transient MqttAsyncClient mqttClient = null;

    // If source should immediately acknowledge incoming message
    private boolean autoAck;

    private SourceContext<OUT> flinkCtx = null;

    // Map of message ids to currently unacknowledged MQTT messages
    private HashMap<String, MqttMessage> unacknowledgedMessages = new HashMap<>();

    // Persistence layer for MQTT client
    private static MemoryPersistence MQTT_persistence;

    // Listener for AMQ exceptions
    //private MQTTExceptionListener exceptionListener;

    /**
     *
     * this callback is invoked upon losing the MQTT connection.
     * @param t
     */
    @Override
    public void connectionLost(Throwable t) {
        System.out.println("Connection lost!");
        // code to reconnect to the broker would go here if desired
        try {
            mqttClient.connect(connOpts).waitForCompletion(1000 * 60);
        } catch (MqttSecurityException se) {
            se.printStackTrace();
            System.out.println("Could not connect to MQTT broker, wrong credentials");
        } catch (MqttException e) {
            e.printStackTrace();
            System.out.println("Could not get to MQTT broker");
        } catch(Exception e) {
            System.out.println("This is definitively not good");
        }
        try {
            // now resubscribe
            mqttClient.subscribe(topicName, QoS);
        } catch(Exception e) {
            System.out.println("This is also bad");

        }
        // we probably need to reestablish the subscriptions
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


        blockingQueue.add(message);
    }

    /**
     * Create MQTTSource.
     *
     * @param config MQTTSource configuration
     */
    public MQTTSource(MQTTSourceConfig<OUT> config) {
        super(String.class);

        this.brokerURL = config.getBrokerURL();
        this.topicName = config.getTopicName();
        this.deserializationSchema = config.getDeserializationSchema();
        this.runningChecker = config.getRunningChecker();
        this.userName = config.getUserName();
        this.password = config.getPassword();
        this.clientId = config.getClientId();
        //System.out.println("Broker: " + this.brokerURL);
        //System.out.println("Topic: " + this.topicName);
        //System.out.println("ClientID: " + this.clientId);


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
    //void setExceptionListener(MQTTExceptionListener exceptionListener) {
    //    this.exceptionListener = exceptionListener;
    //}

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);

        this.connOpts = new MqttConnectOptions();
        this.blockingQueue = new ArrayBlockingQueue<MqttMessage>(10);
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(30);

        // set user credentials
        connOpts.setUserName(this.userName);
        connOpts.setPassword(this.password.toCharArray());
        connOpts.setCleanSession(true);
        //connOpts.setKeepAliveInterval(60); default
        //connOpts.setMaxInflight(10); default, only for publish
        connOpts.setAutomaticReconnect(false); // we do it and resubscribe


        MQTT_persistence = new MemoryPersistence();

        // Create a Connection
        try {
            mqttClient = new MqttAsyncClient(brokerURL, clientId, MQTT_persistence);
            mqttClient.setCallback(this);
            mqttClient.setManualAcks(true);

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            //LoggerUtility.info(CLASS_NAME, METHOD, "Provider: " + sslContext.getProvider().getName());
            sslContext.init(null, null, null);
            connOpts.setSocketFactory(sslContext.getSocketFactory());

            mqttClient.connect(connOpts).waitForCompletion(1000 * 60);
        } catch (MqttSecurityException se) {
            se.printStackTrace();
            System.out.println("Could not connect to MQTT broker, wrong credentials");
            throw se;
        } catch (MqttException e) {
            e.printStackTrace();
            System.out.println("Could not get to MQTT broker");
            throw e;
        }

        //exceptionListener = new AMQExceptionListener(LOG, logFailuresOnly);
        //connection.setExceptionListener(exceptionListener);

        RuntimeContext runtimeContext;
        try {
            runtimeContext = getRuntimeContext();
        } catch (Exception e) {
            QoS = 2;
            autoAck = false;
            return;
        } finally {
            runningChecker.setIsRunning(true);
        }
        if (runtimeContext instanceof StreamingRuntimeContext
                && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
            autoAck = false;
            QoS = 2;
        } else {
            autoAck = true;
            QoS = 1;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        RuntimeException exception = null;
        try {
            if (mqttClient != null) {
                mqttClient.close();
            }
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
        System.out.println("subscribing to " + topicName + "   QoS: " + QoS);
        //exceptionListener.checkErroneous();


        try {
            autoAck = false;
            mqttClient.subscribe(topicName, QoS);
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (runningChecker.isRunning()) {
            MqttMessage message = blockingQueue.take();

            if (message == null) return;

            byte[] bytes = message.getPayload();

            System.out.println("-------------- main thread ----------------------");
            System.out.println("| Message: " + new String(message.getPayload()));
            System.out.println("-------------------------------------------------");

            if (flinkCtx == null) {continue;}

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
    }

    @Override
    public void cancel() {

        runningChecker.setIsRunning(false);
        blockingQueue.add(null); // wake up runner

    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    /* to test locally in the IDE's debugger */
    /*
    public static void main(String[] args) {

        DeserializationSchema<String> deserializationSchema = new SimpleStringSchema();

        RunningChecker rc = new RunningChecker();

        MQTTSourceConfig mqttSC = new MQTTSourceConfig(
                                "ssl://c8j2xj.messaging.internetofthings.ibmcloud.com:8883",
                                "a-c8j2xj-lonlpr4flw", "eDI7Ar-d_p2f0V3fuq",
                                "a:c8j2xj:a-c8j2xj-lonlpr4flw",
                                deserializationSchema,rc,
                                "iot-2/type/+/id/+/evt/+/fmt/+");

        MQTTSource<String> smc = new MQTTSource<String>(mqttSC);
        try {
            smc.open(null);
        } catch (Exception e) {
            System.exit(2);
        }

        try {
            smc.run(null);
        } catch (Exception e) {
            System.exit(3);
        }
    } */

}
