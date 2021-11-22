# Kafka Connect JDBC Connector
Please see the [confluentinc/kafka-connect-jdbc](https://github.com/confluentinc/kafka-connect-jdbc) GitHub repository for the official connector. This is a fork of that repository that adds a new feature explained below.

# Kafka Connect JDBC Continuum Connector
This connector expands on the Kafka connect JDBC connector developed by Confluent. It adds a new feature where you could specify a topic to send JDBC status updates whether successful or not.

For example, lets say you are developing an application for using websockets and you have a change data capture process implimented using the JDBC source / sink connector, how would you communicate to the websocket service that a JDBC source or sink event completed? Using this fork, it allows you to specify a topic to send status updates to continue the event pipeline.

# Continuum Properties
Here are the following new properties for both the source and sink connector:
* `continuum.topic` - The name of topic to produce the JDBC record status update to.
* `continuum.bootstrap.servers` - The Kafka connector framework currently does not allow access to the properties given in the config/connector.properties, so we have to indicate the bootstrap properties when configuring the source or sink connector.
* `continuum.schema.registry.url` - The records created in the continuum topic are written in AVRO, so schema registry is required.
* `continuum.label` - The records created in the continuum topic contains a target property that indicate what process created the event. By default this is set to `JdbcConnector` for both the source and sink connector. This property is recommend if you have more than on JDBC connector writing to the same topic to clearify what event completed.

# Examples
## Example Sink connector configuration:
```
{
    "name": "my_sink_connector",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "topics": "my_records",
        "connection.url": "jdbc:postgresql://my_sink_server:5432/my_sink_database",
        "connection.user": "db_user",
        "connection.password": "db_password",
        "key.converter": "org.apache.kafka.connect.converters.IntegerConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schemas.enabled": true,
        "value.converter.schema.registry.url": "http://my_schema_registry:8081",
        "tasks.max":"1",
        "auto.create": true,
        "auto.evolve": true,
        "insert.mode": "upsert",
        "pk.mode": "record_key",
        "pk.fields": "acct_id",
        "poll.interval.ms" : 500,

        "continuum.topic": "my_records_status",
        "continuum.bootstrap.servers": "broker1:9092,broker2:9092",
        "continuum.schema.registry.url": "http://my_schema_registry:8081"
    }
}
```
## Example Events in the Continuum Topic
* Success
```
rowtime: 2021/11/22 05:23:28.620 Z, key: 79, value: {"target": "JdbcConnector", "statusCode": 200}
```

* Failure
```
rowtime: 2021/11/22 05:23:28.620 Z, key: 79, value: {"target": "JdbcConnector", "statusCode": 500}
```

# Build Repo
* Install all the components needed to compile the Confluent JDBC code or use this convenient GitHub repo that does all the steps for you.
```
git clone https://github.com/enots227/confluent-src-dev-env.git

cd <github repos path>/confluent-src-dev-env

docker build -t confluent_src_dev_env .

docker run -it -v /c/Users/<User>/<Confluent Java Source Code>:/mnt/src confluent_src_dev_env  bash
```
* Build source code using maven.
```
cd <source code path or /mnt/src if confluent_src_dev_env docker image>
mvn clean install -DskipTests
```