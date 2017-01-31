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

import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

/**
 * Immutable AMQ source config.
 *
 * @param <IN> type of source output messages
 */
public class MQTTSinkConfig<IN> {

    //private final ActiveMQConnectionFactory connectionFactory;
    private final String brokerURL;
    private final String userName;
    private final String password;
    private final String clientId;
    private final SerializationSchema<IN> serializationSchema;
    private final RunningChecker runningChecker;
    private final String topicName;

    //MQTTSinkConfig(ActiveMQConnectionFactory connectionFactory, String destinationName,
    public MQTTSinkConfig(String brokerURL,
                     String userName,
                     String password,
                     String clientId,
                     SerializationSchema<IN> serializationSchema,
                     RunningChecker runningChecker,
                     String topicName) {
        //this.connectionFactory = Preconditions.checkNotNull(connectionFactory, "connectionFactory not set");
        this.brokerURL = Preconditions.checkNotNull(brokerURL, "brokerURL not set");
        this.userName = userName;
        this.password = password;
        this.clientId = clientId;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
        this.runningChecker = Preconditions.checkNotNull(runningChecker, "runningChecker not set");
        this.topicName = Preconditions.checkNotNull(topicName, "topicName not set");
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getClientId() {return clientId; }

    public SerializationSchema<IN> getSerializationSchema() {
        return serializationSchema;
    }

    public RunningChecker getRunningChecker() {
        return runningChecker;
    }

    public String getTopicName() {
        return topicName;
    }

    /**
     * Builder for {@link MQTTSinkConfig}
     *
     * @param <IN> type of source output messages
     */
    public static class MQTTSinkConfigBuilder<IN> {
        //private ActiveMQConnectionFactory connectionFactory;
        private String brokerURL;
        private String userName;
        private String password;
        private String clientId;
        private SerializationSchema<IN> serializationSchema;
        private RunningChecker runningChecker = new RunningChecker();
        private String topicName;

        //public MQTTSinkConfigBuilder<IN> setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        //    this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
        //    return this;
        //}

        public MQTTSinkConfigBuilder<IN> setBrokerURL(String brokerURL) {
            this.brokerURL = Preconditions.checkNotNull(brokerURL);
            return this;
        }

        public MQTTSinkConfigBuilder<IN> setUserName(String userName) {
            this.userName = Preconditions.checkNotNull(userName);
            return this;
        }

        public MQTTSinkConfigBuilder<IN> setPassword(String password) {
            this.password = Preconditions.checkNotNull(password);
            return this;
        }

        public MQTTSinkConfigBuilder<IN> setClientId(String clientId) {
            this.clientId = Preconditions.checkNotNull(clientId);
            return this;
        }


        public MQTTSinkConfigBuilder<IN> setDeserializationSchema(SerializationSchema<IN> serializationSchema) {
            this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
            return this;
        }

        public MQTTSinkConfigBuilder<IN> setRunningChecker(RunningChecker runningChecker) {
            this.runningChecker = Preconditions.checkNotNull(runningChecker);
            return this;
        }

        public MQTTSinkConfigBuilder<IN> setTopicName(String topicName) {
            this.topicName = Preconditions.checkNotNull(topicName);
            return this;
        }

        public MQTTSinkConfig<IN> build() {
            return new MQTTSinkConfig<IN>(brokerURL, userName, password, clientId,
                    serializationSchema, runningChecker, topicName);
        }

    }
}
