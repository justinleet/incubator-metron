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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.metron.common.Constants;
import org.apache.metron.integration.UnableToStartException;
import org.apache.metron.integration.components.KafkaComponent;
import org.apache.metron.integration.components.KafkaComponent.Topic;
import org.apache.metron.integration.components.ZKServerComponent;
import org.apache.metron.integration.utils.TestUtils;

/**
 * Meant to mirror the UnifiedEnrichmentBolt in terms of performing enrichments.
 */

public class EnrichmentPipelineTest {

  public static final String SAMPLE_DATA_PARSED_PATH = "metron-platform/metron-integration-test/src/main/sample/data/test/parsed/";
  public static final String sampleParsedPath = SAMPLE_DATA_PARSED_PATH + "TestExampleParsed";
  private static ZKServerComponent zkComponent;
  static final Properties topologyProperties = new Properties();

  public static void main(String[] args)
      throws UnableToStartException, InterruptedException, IOException {
    List<byte[]> inputMessages = getInputMessages(sampleParsedPath);
    zkComponent = getZKServerComponent(topologyProperties);
    zkComponent.start();
    final KafkaComponent kafkaComponent = new KafkaComponent()
        .withTopics(new ArrayList<Topic>() {
                      {
                        add(new KafkaComponent.Topic(Constants.ENRICHMENT_TOPIC, 1));
                        add(new KafkaComponent.Topic(Constants.INDEXING_TOPIC, 1));
                      }
                    }
        ).withTopologyProperties(topologyProperties)
        .withExistingZookeeper(zkComponent.getConnectionString());
    kafkaComponent.start();
    kafkaComponent.writeMessages(Constants.ENRICHMENT_TOPIC, inputMessages);

    PipelineOptions options = PipelineOptionsFactory.create();
    EnrichmentPipelineBuilder builder = new EnrichmentPipelineBuilder();
    builder.setEnrichmentTopic(Constants.ENRICHMENT_TOPIC);
    builder.setIndexingTopic(Constants.INDEXING_TOPIC);
    builder.setKafkaBrokerList(kafkaComponent.getBrokerList());
    builder.setPipelineOptions(options);
    builder.setKafkaConfig(
        new HashMap<String, Object>() {
          {
            put("auto.offset.reset", "earliest");
          }
        }
    );
//    builder.setStrategy()
//    builder.setZookeeper()
    Pipeline p = builder.build();

    PipelineResult stuff = p.run();
    Thread.sleep(8000L); // TODO anything but this
    stuff.cancel();

    List<byte[]> messages = kafkaComponent.readMessages(Constants.INDEXING_TOPIC);
    System.out.println("OUTPUT SIZE:" + messages.size());
    for (byte[] message : messages) {
      System.out.println("OUTPUT MESSAGE:" + new String(message));
    }
    kafkaComponent.stop();
    zkComponent.stop();
    System.exit(0);
  }

  private static List<byte[]> getInputMessages(String path) {
    try {
      return TestUtils.readSampleData(path);
    } catch (IOException ioe) {
      return null;
    }
  }

  private static ZKServerComponent getZKServerComponent(final Properties topologyProperties) {
    return new ZKServerComponent()
        .withPostStartCallback((zkComponent) -> {
              topologyProperties
                  .setProperty(
                      ZKServerComponent.ZOOKEEPER_PROPERTY,
                      zkComponent.getConnectionString()
                  );
              topologyProperties.setProperty("kafka.zk", zkComponent.getConnectionString());
            }
        );
  }
}
