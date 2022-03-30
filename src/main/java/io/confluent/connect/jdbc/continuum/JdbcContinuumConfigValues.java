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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class JdbcContinuumConfigValues {
  private static final Logger log = LoggerFactory.getLogger(JdbcContinuumConfigValues.class);

  public String topic;
  public String label;
  public String bootstrapServers;
  public String schemaRegistryURL;
  public String versionColumnName;
  public String updatedOnColumnName;

  public boolean isConfigured() {
    if (topic != "" || bootstrapServers != "" || schemaRegistryURL != "") {
      ArrayList<String> missingValues = new ArrayList<String>();

      if (topic == "") {
        missingValues.add(JdbcContinuumConfig.CONTINUUM_TOPIC_CONFIG);
      }
      if (bootstrapServers == "") {
        missingValues.add(JdbcContinuumConfig.CONTINUUM_BOOTSTRAP_SERVERS_CONFIG);
      }
      if (schemaRegistryURL == "") {
        missingValues.add(JdbcContinuumConfig.CONTINUUM_SCHEMA_REGISTRY_URL_CONFIG);
      }

      if (missingValues.size() > 0) {
        log.warn("Continuum properties are partially configured. The following "
            + "properties need to be configured to work: {}",  
            String.join(",", missingValues));
        return false;
      }

      return true;
    }

    log.info("Continuum properties are not configured.");
    return false;
  }
}
