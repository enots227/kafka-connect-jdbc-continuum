/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.continuum;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Date;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class JdbcContinuum {
  private static final Logger log = LoggerFactory.getLogger(JdbcContinuum.class);

  private Producer<Object, Object> producer;
  private String topic;
  private String label;
  private Schema valueSchema;

  public static enum Outcome { SUCCESS, FAILURE }

  public JdbcContinuum(AbstractConfig config) {
    final JdbcContinuumConfigValues continuumConfig = JdbcContinuumConfig.parseConfigValues(config);

    if (continuumConfig.isConfigured()) {
      Properties props = new Properties();
      props.put(BOOTSTRAP_SERVERS_CONFIG, continuumConfig.bootstrapServers);
      props.put(SCHEMA_REGISTRY_URL_CONFIG, continuumConfig.schemaRegistryURL);
      props.put(KEY_SERIALIZER_CLASS_CONFIG,
          org.apache.kafka.common.serialization.StringSerializer.class);
      props.put(VALUE_SERIALIZER_CLASS_CONFIG,
          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
      producer = new KafkaProducer<>(props);

      String statusSchema = 
          "{\"type\":\"record\","
          + "\"name\":\"" + continuumConfig.topic + "_continuum\","
          + "\"namespace\":\"io.confluent.connect.jdbc.continuum\","
          + "\"fields\":["
          + "{\"name\":\"label\",\"type\":\"string\"},"
          + "{\"name\":\"outcome\",\"type\":\"int\"},"
          + "{\"name\":\"version\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"updatedOn\",\"type\":[\"null\",{\"type\":\"long\","
          + "\"logicalType\":\"timestamp-millis\"}],\"default\":null}"
          + "]}";
      Schema.Parser parser = new Schema.Parser();
      valueSchema = parser.parse(statusSchema);

      topic = continuumConfig.topic;
      label = continuumConfig.label;

      log.info("Created Continuum producer with topic {}", topic);
    } else {
      log.info("No Continuum producer created");
    }
  }

  public boolean isActive() {
    return producer != null;
  }

  public int outcomeToInt(Outcome outcome) {
    switch (outcome) {
      case SUCCESS:
        return 1;
      default:
        return 0;
    }
  }

  public void produce(String key, Outcome outcome, String version, Date updatedOn) {
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("label", label);
    value.put("outcome", outcomeToInt(outcome));
    value.put("version", version);
    value.put("updatedOn", updatedOn != null ? updatedOn.getTime() : null);

    producer.send(new ProducerRecord<Object, Object>(topic, key, value));
  }

  public void stop() {
    if (producer != null) {
      log.debug("Stopping JdbcContinuum... Continuum producer detected, closing producer.");
      try {
        producer.close();
      } catch (Throwable t) {
        log.warn("Error while closing the {} continuum producer: ", label, t);
      } finally {
        producer = null;
      }
    }
  }
}
