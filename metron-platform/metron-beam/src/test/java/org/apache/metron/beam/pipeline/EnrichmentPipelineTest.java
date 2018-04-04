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
import java.util.Map;
import java.util.Properties;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.Configurations;
import org.apache.metron.common.configuration.ConfigurationsUtils;
import org.apache.metron.common.configuration.enrichment.EnrichmentConfig;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentUpdateConfig;
import org.apache.metron.integration.UnableToStartException;
import org.apache.metron.integration.components.KafkaComponent;
import org.apache.metron.integration.components.KafkaComponent.Topic;
import org.apache.metron.integration.components.ZKServerComponent;
import org.apache.metron.integration.utils.TestUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Meant to mirror the UnifiedEnrichmentBolt in terms of performing enrichments.
 */

public class EnrichmentPipelineTest {

  /**
  {
    "enrichment": {
      "fieldMap": {
        "stellar" : {
          "config" : {
            "stmt1" : "TO_UPPER(source.type)"
          }
        }
      }
    }
   }
  */
  @Multiline
  public static String enrichmentConfigOne;

  /**
   {
     "enrichment": {
       "fieldMap": {
         "stellar" : {
           "config" : {
             "stmt1" : "TO_UPPER(source.type)",
             "stmt2" : "TO_LOWER(iflags)"
           }
         }
       }
     }
   }
   */
  @Multiline
  public static String enrichmentConfigTwo;

//  /**
//   {
//   "fieldMap": {
//   "stellar" : {
//   "config" : {
//   "stmt1" : "TO_UPPER(source.type)"
//   }
//   }
//   }
//   }
//   */
//  @Multiline
//  public static String stellarConfigMapOne;
//
//
//  /**
//   {
//   "fieldMap": {
//   "stellar" : {
//   "config" : {
//   "stmt1" : "TO_UPPER(source.type)",
//   "stmt2" : "TO_LOWER(iflags)"
//   }
//   }
//   }
//   }
//   */
//  @Multiline
//  public static String stellarConfigMapTwo;

  public static final String SAMPLE_DATA_PARSED_PATH = "metron-platform/metron-integration-test/src/main/sample/data/test/parsed/";
  public static final String sampleParsedPath = SAMPLE_DATA_PARSED_PATH + "TestExampleParsed";
  private static ZKServerComponent zkComponent;
  static final Properties topologyProperties = new Properties();

  public static void main(String[] args)
      throws Exception {
    List<byte[]> inputMessages = getInputMessages(sampleParsedPath);
    zkComponent = getZKServerComponent(topologyProperties);
    zkComponent.start();
    JSONParser parser = new JSONParser();
    JSONObject configOne = (JSONObject) parser.parse(enrichmentConfigOne);
    JSONObject configTwo = (JSONObject) parser.parse(enrichmentConfigTwo);
    Configurations configurations = new Configurations();
    configurations.updateGlobalConfig(configOne);
    // The hell is this nonsense?
    SensorEnrichmentConfig sensorEnrichmentConfig = new SensorEnrichmentConfig();
    EnrichmentConfig enrichmentConfig = new EnrichmentConfig();
    enrichmentConfig.setFieldMap((Map<String, Object>)((JSONObject)configOne.get("enrichment")).get("fieldMap"));
    sensorEnrichmentConfig.setEnrichment(enrichmentConfig);
    ConfigurationsUtils.writeGlobalConfigToZookeeper(configurations.getConfigurations(), zkComponent.getConnectionString());
    ConfigurationsUtils.writeSensorEnrichmentConfigToZookeeper("test", sensorEnrichmentConfig, zkComponent.getConnectionString());

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
    kafkaComponent.writeMessages(Constants.ENRICHMENT_TOPIC, inputMessages.subList(0, 5));

    PipelineOptions options = PipelineOptionsFactory.create();
    EnrichmentPipelineBuilder builder = new EnrichmentPipelineBuilder();
    builder.setEnrichmentTopic(Constants.ENRICHMENT_TOPIC);
    builder.setIndexingTopic(Constants.INDEXING_TOPIC);
    builder.setKafkaBrokerList(kafkaComponent.getBrokerList());
    builder.setPipelineOptions(options);
    builder.setZookeeper(zkComponent.getConnectionString());
    builder.setKafkaConfig(
        new HashMap<String, Object>() {
          {
            put("auto.offset.reset", "earliest");
          }
        }
    );
    Pipeline p = builder.build();

    PipelineResult stuff = p.run();

//    Thread.sleep(8000L);

//    // Write more messages to Kafka after we've updated ZK
//    System.out.println("UPDATING ZK CONFIG");
//    configurations = new Configurations();
//    configurations.updateGlobalConfig(configTwo);
//    // The hell is this nonsense?
//    sensorEnrichmentConfig = new SensorEnrichmentConfig();
//    enrichmentConfig = new EnrichmentConfig();
//    enrichmentConfig.setFieldMap((Map<String, Object>)((JSONObject)configOne.get("enrichment")).get("fieldMap"));
//    sensorEnrichmentConfig.setEnrichment(enrichmentConfig);
//    ConfigurationsUtils.writeGlobalConfigToZookeeper(configurations.getConfigurations(), zkComponent.getConnectionString());
//    ConfigurationsUtils.writeSensorEnrichmentConfigToZookeeper("test", sensorEnrichmentConfig, zkComponent.getConnectionString());
//    kafkaComponent.writeMessages(Constants.ENRICHMENT_TOPIC, inputMessages.subList(5, 10));

    Thread.sleep(8000L); // TODO anything but this
    stuff.cancel();

    System.out.println("POST UPDATE");
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
