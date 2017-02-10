# Apache Bahir (Flink)

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark™ and Apache Flink®.

<http://bahir.apache.org/>


This repository is for Apache Flink extensions.

## Contributing a Flink Connector

The Bahir community is very open to new connector contributions for Apache Flink.

We ask contributors to first open a [JIRA issue](http://issues.apache.org/jira/browse/BAHIR) describing the planned changes. Please make sure to put "Flink Streaming Connector" in the "Component/s" field.

Once the community has agreed that the planned changes are suitable, you can open a pull request at the "bahir-flink" repository.
Please follow the same directory structure as the existing code.

The community will review your changes, giving suggestions how to improve the code until we can merge it to the main repository.



## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/).
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test

### Installing the libraries in your local Maven repository

Since the MQTT connector is not part of the Bahir-Flink distribution, you have to install it in your local Maven repository to build Flink jobs with it.
This is the command I'm using right now

    mvn install:install-file -Dfile=flink-connector-mqtt/target/flink-connector-mqtt_2.11-1.0.0-SNAPSHOT.jar -DgroupId=org.apache.flink -DartifactId=flink-connector-mqtt -Dversion=1.0 -Dpackaging=jar

Since MQTT in general doesn't allow for multiple subscriptions for the same userid/password (API key/secret) pair, it should be used with care. Generally a separate mqttSource or mqttSink object with distinct userids are required in order to add them as source, resp. sink to a datastream.

Furthermore it should be noted that the 'exactly once' semantic depends on the event publisher as well as on the subscriber. So MQTTSource attempts to subscribe with QoS of 2 for 'exactly once', but if message is sent with a lower quality of service, it is received with this QoS.
