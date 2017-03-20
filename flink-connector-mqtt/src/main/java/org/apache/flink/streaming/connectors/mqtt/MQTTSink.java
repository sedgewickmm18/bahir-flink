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
 * WITHIN WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
//import org.apache.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
//import org.apache.flink.streaming.connectors.mqtt.internal.MQTTUtil;
import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
//import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.IOException;
import java.util.HashMap;
//import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.net.URL;
import java.net.URLConnection;
import java.io.IOException;
import javax.net.ssl.SSLContext;

/**
 * Source class for writing data into MQTT queue.
 * <p>
 * To create an instance of MQTTSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * Every input message is converted into a byte array using a serialization
 * schema and being sent into a message queue.
 *
 * @param <IN> type of output messages
 */
public class MQTTSink<IN> extends RichSinkFunction<IN> implements MqttCallback {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSink.class);

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

    // Serialization scheme that is used to convert input message to bytes
    private final SerializationSchema<IN> serializationSchema;

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
        LOG.error("Connection lost!");

        // reconnect in a separate thread
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (;;) {
                    //
                    if (!isInetAvailable()) {
                        try {
                            Thread.sleep(2);
                        } catch (Exception ie) {
                            // ignore interrupt exception
                        }
                        continue;
                    }

                    // reconnect
                    if (!mqttClient.isConnected()) {
                        try {
                            mqttClient.connect(connOpts).waitForCompletion(1000 * 60);
                        } catch (MqttSecurityException se) {
                            se.printStackTrace();
                            LOG.error("Could not connect to MQTT broker, wrong credentials");
                            System.exit(1);
                        } catch (MqttException e) {
                            e.printStackTrace();
                            LOG.error("Could not get to MQTT broker, retry:" + e.getMessage());
                            try {
                                Thread.sleep(3);
                            } catch (Exception ie) {
                                // ignore interrupt exception
                            }
                        } catch(Exception e) {
                            LOG.error("This is definitively not good");
                            System.exit(2);
                        }
                        continue;
                    }
                    break;
                }
            }

        }).start();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        LOG.info("| Topic:" + topic);
        LOG.info("| Message: " + new String(message.getPayload()));
    }


    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        try {
            LOG.info("Pub complete" + new String(token.getMessage().getPayload()));
        }
        catch (MqttException e) {
            LOG.error("MSG DELIVERY FAILED " + e.getMessage());
            try {
                close();
            }
            catch(Exception ee) {
                LOG.error("CANNOT EVEN CLOSE MQTT CLIENT " + ee.getMessage());
            }
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Create MQTTSink
     *
     * @param config MQTTSink configuration
     */
    public MQTTSink(MQTTSinkConfig<IN> config) {

        this.brokerURL = config.getBrokerURL();
        this.topicName = config.getTopicName();
        this.serializationSchema = config.getSerializationSchema();
        this.runningChecker = config.getRunningChecker();
        this.userName = config.getUserName();
        this.password = config.getPassword();
        this.clientId = config.getClientId();
        //System.out.println("Broker: " + this.brokerURL);
        //System.out.println("Topic: " + this.topicName);
        //System.out.println("ClientID: " + this.clientId);
        SimpleStringSchema unusedObject;

        LOG.debug("debug");
        LOG.info("info");
        LOG.error("error");
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
        connOpts.setKeepAliveInterval(30); // default is 60, reduced to 30 to keep firewalls happy
        //connOpts.setMaxInflight(10); default, only for publish
        connOpts.setAutomaticReconnect(false); // we do it and resubscribe


        MQTT_persistence = new MemoryPersistence();

        // Create a Connection
        try {
            mqttClient = new MqttAsyncClient(brokerURL, clientId, MQTT_persistence);
            mqttClient.setCallback(this);
            mqttClient.setManualAcks(true);

            if (brokerURL.startsWith("ssl")) {
                SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
                //LoggerUtility.info(CLASS_NAME, METHOD, "Provider: " + sslContext.getProvider().getName());
                sslContext.init(null, null, null);
                connOpts.setSocketFactory(sslContext.getSocketFactory());
            }

            mqttClient.connect(connOpts).waitForCompletion(1000 * 60);
        } catch (MqttSecurityException se) {
            se.printStackTrace();
            LOG.error("Could not connect to MQTT broker, wrong credentials");
            throw se;
        } catch (MqttException e) {
            e.printStackTrace();
            LOG.error("Could not get to MQTT broker");
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
        QoS = 0; // TODO remove that
    }


    /**
     * Called when new data arrives to the sink, and forwards it to RMQ.
     *
     * @param value
     *            The incoming data
     */
    @Override
    public void invoke(IN value) {
        LOG.info("Invoke");
        try {
            byte[] bytes = serializationSchema.serialize(value);

            MqttMessage pubmsg = new MqttMessage(bytes);
            LOG.info("Publish msg " + pubmsg.getPayload() + " to " + topicName);

            // CAVEAT: this is an asynchronous call, need to understand whether I have to block or do something else here
            mqttClient.publish(topicName, bytes, QoS, false);

        } catch (MqttSecurityException se) {
            LOG.error("Not allowed to publish to MQTT topic: " + topicName);
        } catch (MqttException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to send message to MQTT", e);
            } else {
                throw new RuntimeException("Failed to send message to MQTT", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("Closing MQTT Sink");
        RuntimeException exception = null;
        try {
            if (mqttClient != null) {
                mqttClient.disconnectForcibly();
            }
        } catch (MqttException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close MQTT session", e);
            } else {
                exception = new RuntimeException("Failed to close MQTT consumer", e);
            }
        }

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
        } finally {
            mqttClient = null;
        }

        if (exception != null) {
            throw exception;
        }
    }

    /*Check internet connection*/
    public static boolean isInetAvailable() {
        boolean connectivity;
        try {
            URL url = new URL("https://internetofthings.ibmcloud.com");
            URLConnection conn = url.openConnection();
            conn.connect();
            connectivity = true;
        } catch (IOException e) {
            connectivity = false;
            LOG.error("internet gone");
        }
        return connectivity;
    }

}
