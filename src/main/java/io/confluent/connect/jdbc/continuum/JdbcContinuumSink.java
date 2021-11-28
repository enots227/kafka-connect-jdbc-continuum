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
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class JdbcContinuumSink extends JdbcContinuum {
  private static final Logger log = LoggerFactory.getLogger(JdbcContinuumSink.class);

  public JdbcContinuumSink(AbstractConfig config) {
    super(config);
  }

  public void continueOn(Collection<SinkRecord> records, Integer statusCode) {
    if (isActive()) {
      try {
        log.debug("Signalling sink continuum for {} records with status code {}", 
            records.size(), statusCode);
        for (SinkRecord record : records) {
          log.trace("Signalling sink continuum for {}", record.value());

          String key = record.key().toString();
          
          produce(key, statusCode);
        }
      } catch (IllegalStateException e) {
        log.error("Fatal: Cannot produce Continuum record", e);
        stop();
      }
    }
  }
}
