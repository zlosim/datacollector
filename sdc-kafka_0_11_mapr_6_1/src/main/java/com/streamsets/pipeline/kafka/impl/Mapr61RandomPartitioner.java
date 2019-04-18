/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.kafka.impl;


import org.apache.kafka.clients.producer.StreamsPartitioner;

import java.util.Map;
import java.util.Random;

public class Mapr61RandomPartitioner implements StreamsPartitioner {

  private final Random random = new Random();

  @Override
  public int partition(
      String topic,
      Object key,
      byte[] keyBytes,
      Object value,
      byte[] valueBytes,
      int numPartitions
  ) {
    return random.nextInt(numPartitions);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
