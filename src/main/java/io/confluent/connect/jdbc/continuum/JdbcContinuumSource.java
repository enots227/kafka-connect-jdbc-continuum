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

import java.util.Collection;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

public class JdbcContinuumSource extends JdbcContinuum {
  private static final Logger log = LoggerFactory.getLogger(JdbcContinuumSource.class);

  private String incrementingColumnName;

  public JdbcContinuumSource(AbstractConfig config) {
    super(config);

    if (isActive()) {
      incrementingColumnName = config.getString(
          JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    }
  }

  public void continueOn(Collection<SourceRecord> records, Integer statusCode) {
    if (isActive()) {
      try {
        log.debug("Signalling source continuum for {} records with status code {}", 
            records.size(), statusCode);
        for (SourceRecord record : records) {
          log.trace("Signalling source continuum for {}", record.value());

          Struct recordValue = (Struct) record.value();
          String key = recordValue.get(incrementingColumnName).toString();

          produce(key, statusCode);
        }
      } catch (IllegalStateException e) {
        log.error("Fatal: Cannot produce Continuum record", e);
        close();
      }
    }
  }
}
