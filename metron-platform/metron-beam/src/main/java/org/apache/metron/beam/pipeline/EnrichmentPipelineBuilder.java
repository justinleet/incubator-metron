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
import org.apache.metron.beam.transform.JsonParse;
import org.apache.metron.beam.transform.StellarTransform;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;

public class EnrichmentPipelineBuilder {

  private String zookeeper;
  private Map<String, Object> kafkaConfig;
  private String enrichmentTopic;
  private String indexingTopic;
  private String kafkaBrokerList;
  private EnrichmentStrategies strategy;
  private PipelineOptions pipelineOptions;

  public EnrichmentPipelineBuilder() {

  }

  public EnrichmentPipelineBuilder setZookeeper(String zookeeper) {
    this.zookeeper = zookeeper;
    return this;
  }

  public EnrichmentPipelineBuilder setKafkaConfig(Map<String, Object> kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
    return this;
  }

  public EnrichmentPipelineBuilder setEnrichmentTopic(String enrichmentTopic) {
    this.enrichmentTopic = enrichmentTopic;
    return this;
  }

  public EnrichmentPipelineBuilder setIndexingTopic(String indexingTopic) {
    this.indexingTopic = indexingTopic;
    return this;
  }

  public EnrichmentPipelineBuilder setKafkaBrokerList(String kafkaBrokerList) {
    this.kafkaBrokerList = kafkaBrokerList;
    return this;
  }

  public EnrichmentPipelineBuilder setStrategy(String strategy) {
    this.strategy = EnrichmentStrategies.valueOf(strategy);
    return this;
  }

  public EnrichmentPipelineBuilder setPipelineOptions(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    return this;
  }

  public Pipeline build() {
    if (kafkaConfig == null) {
      kafkaConfig = new HashMap<>();
    }

    Pipeline p = Pipeline.create(pipelineOptions);

    PCollection<KV<String, String>> output = p.apply(
        "Metron-Enrichments",
        KafkaIO.<byte[], byte[]>read()
            .withBootstrapServers(kafkaBrokerList)
            .withTopic(
                enrichmentTopic)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .updateConsumerProperties(kafkaConfig)
            .withMaxNumRecords(10)
    )
        .apply(ParDo.of(new JsonParse()))
        .apply(ParDo.of(new StellarTransform()));
    output
        .apply(KafkaIO.<String, String>write().withBootstrapServers(kafkaBrokerList)
            .withTopic(indexingTopic)
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class));

    return p;
  }
}
