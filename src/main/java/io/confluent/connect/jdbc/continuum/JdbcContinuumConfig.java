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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;

public class JdbcContinuumConfig {
  private static final String CONTINUUM_GROUP = "Continuum";

  public static final String CONTINUUM_TOPIC_CONFIG = 
      "continuum.topic";
  private static final String CONTINUUM_TOPIC_DOC =
      "The topic to send events to once the record is processed.";
  private static final String CONTINUUM_TOPIC_DISPLAY =
      "Continuum Topic";

  public static final String CONTINUUM_LABEL_CONFIG =
      "continuum.label";
  private static final String CONTINUUM_LABEL_DOC =
      "The label associated with the completion of a connector task.";
  private static final String CONTINUUM_LABEL_DISPLAY =
      "Continuum Label";

  public static final String CONTINUUM_BOOTSTRAP_SERVERS_CONFIG =
      "continuum.bootstrap.servers";
  private static final String CONTINUUM_BOOTSTRAP_SERVERS_DOC =
      "The initial Kafka brokers to established connection to for the continuum topic.";
  private static final String CONTINUUM_BOOTSTRAP_SERVERS_DISPLAY = 
      "Continuum Bootstrap Servers";

  public static final String CONTINUUM_SCHEMA_REGISTRY_URL_CONFIG =
      "continuum.schema.registry.url";
  private static final String CONTINUUM_SCHEMA_REGISTRY_URL_DOC =
      "The schema registry service.";
  private static final String CONTINUUM_SCHEMA_REGISTRY_URL_DISPLAY = 
      "Continuum Schema Registry";

  public static final String CONTINUUM_VERSION_COLUMN_NAME_CONFIG =
      "continuum.version.column.name";
  private static final String CONTINUUM_VERSION_COLUMN_NAME_DOC =
      "The column name that stores the record version.";
  private static final String CONTINUUM_VERSION_COLUMN_NAME_DISPLAY = 
      "Continuum Version Column Name";

  public static final String CONTINUUM_UPDATED_ON_COLUMN_NAME_CONFIG =
      "continuum.updatedOn.column.name";
  private static final String CONTINUUM_UPDATED_ON_COLUMN_NAME_DOC =
      "The column name that stores the record last updated on datetime.";
  private static final String CONTINUUM_UPDATED_ON_COLUMN_NAME_DISPLAY = 
      "Continuum Last Updated On Column Name";

  public static JdbcContinuumConfigValues parseConfigValues(AbstractConfig config) {
    JdbcContinuumConfigValues values = new JdbcContinuumConfigValues();

    values.topic = config
        .getString(JdbcContinuumConfig.CONTINUUM_TOPIC_CONFIG)
        .trim();
    values.label = config
        .getString(JdbcContinuumConfig.CONTINUUM_LABEL_CONFIG)
        .trim();
    values.bootstrapServers = config
        .getString(JdbcContinuumConfig.CONTINUUM_BOOTSTRAP_SERVERS_CONFIG);
    values.schemaRegistryURL = config
        .getString(JdbcContinuumConfig.CONTINUUM_SCHEMA_REGISTRY_URL_CONFIG);
    values.versionColumnName = config
        .getString(JdbcContinuumConfig.CONTINUUM_VERSION_COLUMN_NAME_CONFIG);
    values.updatedOnColumnName = config
        .getString(JdbcContinuumConfig.CONTINUUM_UPDATED_ON_COLUMN_NAME_CONFIG);

    return values;
  }

  public static ConfigDef continuumDefs(ConfigDef defs) {
    return defs
      .define(
        CONTINUUM_BOOTSTRAP_SERVERS_CONFIG,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.LOW,
        CONTINUUM_BOOTSTRAP_SERVERS_DOC,
        CONTINUUM_GROUP,
        1,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_BOOTSTRAP_SERVERS_DISPLAY
      ).define(
        CONTINUUM_SCHEMA_REGISTRY_URL_CONFIG,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.LOW,
        CONTINUUM_SCHEMA_REGISTRY_URL_DOC,
        CONTINUUM_GROUP,
        2,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_SCHEMA_REGISTRY_URL_DISPLAY
      ).define(
        CONTINUUM_TOPIC_CONFIG,
        ConfigDef.Type.STRING,
        "",
        new Validator() {
          @Override
          public void ensureValid(final String name, final Object value) {
            if (value == null) {
              return;
            }

            String trimmed = ((String) value).trim();

            if (trimmed.length() > 249) {
              throw new ConfigException(name, value,
                  "Continuum topic length must not exceed max topic name length, 249 chars");
            }

            if (JdbcSourceConnectorConfig.INVALID_CHARS.matcher(trimmed).find()) {
              throw new ConfigException(name, value,
                  "Continuum topic must not contain any character other than "
                  + "ASCII alphanumerics, '.', '_' and '-'.");
            }
          }
        },
        ConfigDef.Importance.LOW,
        CONTINUUM_TOPIC_DOC,
        CONTINUUM_GROUP,
        3,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_TOPIC_DISPLAY
      ).define(
        CONTINUUM_LABEL_CONFIG,
        ConfigDef.Type.STRING,
        "JdbcConnector",
        ConfigDef.Importance.LOW,
        CONTINUUM_LABEL_DOC,
        CONTINUUM_GROUP,
        4,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_LABEL_DISPLAY
      ).define(
        CONTINUUM_VERSION_COLUMN_NAME_CONFIG,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.HIGH,
        CONTINUUM_VERSION_COLUMN_NAME_DOC,
        CONTINUUM_GROUP,
        5,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_VERSION_COLUMN_NAME_DISPLAY
      ).define(
        CONTINUUM_UPDATED_ON_COLUMN_NAME_CONFIG,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.HIGH,
        CONTINUUM_UPDATED_ON_COLUMN_NAME_DOC,
        CONTINUUM_GROUP,
        6,
        ConfigDef.Width.MEDIUM,
        CONTINUUM_UPDATED_ON_COLUMN_NAME_DISPLAY);
  }

}
