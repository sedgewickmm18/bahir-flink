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

import org.apache.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;

/**
 * Immutable AMQ source config.
 *
 * @param <OUT> type of source output messages
 */
public class MQTTSourceConfig<OUT> {

    //private final ActiveMQConnectionFactory connectionFactory;
    private final String brokerURL;
    private final String userName;
    private final String password;
    private final String clientId;
    private final DeserializationSchema<OUT> deserializationSchema;
    private final RunningChecker runningChecker;
    private final String[] topicNames;

    //MQTTSourceConfig(ActiveMQConnectionFactory connectionFactory, String destinationName,
    public MQTTSourceConfig(String brokerURL,
                     String userName,
                     String password,
                     String clientId,
                     DeserializationSchema<OUT> deserializationSchema,
                     RunningChecker runningChecker,
                     String [] topicNames) {
        //this.connectionFactory = Preconditions.checkNotNull(connectionFactory, "connectionFactory not set");
        this.brokerURL = Preconditions.checkNotNull(brokerURL, "brokerURL not set");
        this.userName = userName;
        this.password = password;
        this.clientId = clientId;
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema not set");
        this.runningChecker = Preconditions.checkNotNull(runningChecker, "runningChecker not set");
        //this.topicName = Preconditions.checkNotNull(topicName, "topicName not set");
        this.topicNames = topicNames;
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

    public DeserializationSchema<OUT> getDeserializationSchema() {
        return deserializationSchema;
    }

    public RunningChecker getRunningChecker() {
        return runningChecker;
    }

    public String[] getTopicNames() {
        return topicNames;
    }

    /**
     * Builder for {@link AMQSourceConfig}
     *
     * @param <OUT> type of source output messages
     */
    public static class MQTTSourceConfigBuilder<OUT> {
        //private ActiveMQConnectionFactory connectionFactory;
        private String brokerURL;
        private String userName;
        private String password;
        private String clientId;
        private DeserializationSchema<OUT> deserializationSchema;
        private RunningChecker runningChecker = new RunningChecker();
        private String[] topicNames;

        //public MQTTSourceConfigBuilder<OUT> setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        //    this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
        //    return this;
        //}

        public MQTTSourceConfigBuilder<OUT> setBrokerURL(String brokerURL) {
            this.brokerURL = Preconditions.checkNotNull(brokerURL);
            return this;
        }

        public MQTTSourceConfigBuilder<OUT> setUserName(String userName) {
            this.userName = Preconditions.checkNotNull(userName);
            return this;
        }

        public MQTTSourceConfigBuilder<OUT> setPassword(String password) {
            this.password = Preconditions.checkNotNull(password);
            return this;
        }

        public MQTTSourceConfigBuilder<OUT> setClientId(String clientId) {
            this.clientId = Preconditions.checkNotNull(clientId);
            return this;
        }


        public MQTTSourceConfigBuilder<OUT> setDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
            this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema);
            return this;
        }

        public MQTTSourceConfigBuilder<OUT> setRunningChecker(RunningChecker runningChecker) {
            this.runningChecker = Preconditions.checkNotNull(runningChecker);
            return this;
        }

        public MQTTSourceConfigBuilder<OUT> setTopicName(String topicName) {
            //this.topicNames = Preconditions.checkNotNull(topicName);
            this.topicNames = topicNames;
            return this;
        }

        public MQTTSourceConfig<OUT> build() {
            return new MQTTSourceConfig<OUT>(brokerURL, userName, password, clientId,
                    deserializationSchema, runningChecker, topicNames);
        }

    }
}
