# Flink MQTT connector


This connector provides a source, subscribing to any MQTT broker such as provided by IBM's [Watson IoT platform](http://www.ibm.com/internet-of-things/)™.
To use this connector, add the following dependency to your project:


    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-mqtt_2.11</artifactId>
      <version>1.0</version>
    </dependency>

*Version Compatibility*: This module is compatible with MQTT 3.1

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.


The source class is called `MQTTSource`.
<br>
#### ToDos

This is an initial pre-alpha drop that works in a local configuration, nothing more.<br><br>
- Unit tests do not work<br>
- Test in proper Flink deployment needs to be done