/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.metron.beam.pipeline;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.metron.beam.transform.JsonToString;
import org.apache.metron.beam.transform.MessageParser;

public class ParserPipelineBuilder {

  private String zookeeper;
  private Map<String, Object> kafkaConfig;
  private String enrichmentTopic;
  private String parserTopic;
  private String kafkaBrokerList;
  private PipelineOptions pipelineOptions;

  public ParserPipelineBuilder() {

  }

  public ParserPipelineBuilder setZookeeper(String zookeeper) {
    this.zookeeper = zookeeper;
    return this;
  }

  public ParserPipelineBuilder setKafkaConfig(Map<String, Object> kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
    return this;
  }

  public ParserPipelineBuilder setEnrichmentTopic(String enrichmentTopic) {
    this.enrichmentTopic = enrichmentTopic;
    return this;
  }

  public ParserPipelineBuilder setParserTopic(String parserTopic) {
    this.parserTopic = parserTopic;
    return this;
  }

  public ParserPipelineBuilder setKafkaBrokerList(String kafkaBrokerList) {
    this.kafkaBrokerList = kafkaBrokerList;
    return this;
  }

  public ParserPipelineBuilder setPipelineOptions(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    return this;
  }

  public Pipeline build() {
    if (kafkaConfig == null) {
      kafkaConfig = new HashMap<>();
    }

    Pipeline p = Pipeline.create(pipelineOptions);

    PCollection<KV<String, String>> output = p.apply(
        "Metron-Parser",
        KafkaIO.<byte[], byte[]>read()
            .withBootstrapServers(kafkaBrokerList)
            .withTopic(parserTopic)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .updateConsumerProperties(kafkaConfig)
            .withMaxNumRecords(10)
    )
        .apply(ParDo.of(new MessageParser()))
        .apply(ParDo.of(new JsonToString()));
    output
        .apply(KafkaIO.<String, String>write().withBootstrapServers(kafkaBrokerList)
            .withTopic(enrichmentTopic)
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));

    return p;
  }
}
