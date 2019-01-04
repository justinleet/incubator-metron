package org.apache.metron.parsers

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

import java.util.function.Supplier

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.metron.common.configuration.{ConfigurationsUtils, ParserConfigurations, SensorParserConfig}
import org.apache.metron.stellar.dsl.Context.{Capabilities, Capability}
import org.apache.metron.stellar.dsl.{Context, StellarFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.{JavaConversions, mutable}

class ParserApplication {

  /**
    * Consumes messages from one or more topics in Kafka and does wordcount.
    * Usage: DirectKafkaWordCount <brokers> <topics>
    * <brokers> is a list of one or more Kafka brokers
    * <groupId> is a consumer group name to consume from topics
    * <topics> is a list of one or more kafka topics to consume from
    *
    * Example:
    * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
    * consumer-group topic1,topic2
    */
  object DirectKafkaWordCount {


    def main(args: Array[String]) {
      // TODO fix this. For now hardcode the values
      //      if (args.length < 3) {
      //        System.err.println(
      //          s"""
      //             |Usage: DirectKafkaWordCount <brokers> <topics>
      //             |  <brokers> is a list of one or more Kafka brokers
      //             |  <groupId> is a consumer group name to consume from topics
      //             |  <topics> is a list of one or more kafka topics to consume from
      //             |
      //          """.stripMargin)
      //        System.exit(1)
      //      }

      val args = Array("node1:2181", "node1:6667", "bro_parser", "bro")
      var sensorType = "bro"
      val Array(zookeeperUrl, brokers, groupId, topics) = args

      // Create context with 2 second batch interval
      val sparkConf = new SparkConf().setAppName("ParserTest") // TODO Need to set this based on sensor
      val ssc = new StreamingContext(sparkConf, Seconds(2))

      // Create direct kafka stream with brokers and topics
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

      messages.foreachRDD { rdd =>
        rdd.foreachPartition {
          //  FunctionResolver functionResolver;
          //  Map<String, Object> variables;
          //  Context context = null;
          //  StellarStatefulExecutor executor;
          //
          //  @Before
          //  public void setup() {
          //    variables = new HashMap<>();
          //    functionResolver = new SimpleFunctionResolver()
          //            .withClass(ParserFunctions.ParseFunction.class)
          //            .withClass(ParserFunctions.InitializeFunction.class)
          //            .withClass(ParserFunctions.ConfigFunction.class);
          //    context = new Context.Builder().build();
          //    executor = new DefaultStellarStatefulExecutor(functionResolver, context);

          // Build a ZooKeeper Connection
          // Cache nothing for the spike
          // Also ignoring Parser Aggregation. And Chaining. And probably some other features.
          val retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1000, 3)
          var client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
          client.start()

          // Get Global Configs
          var globalConfigs = ConfigurationsUtils.readGlobalConfigFromZookeeper(client())

          // Get Parser Configs
          var configs = new ParserConfigurations()
          val parserConfigs = mutable.HashMap[String, SensorParserConfig]()
          ConfigurationsUtils.updateParserConfigsFromZookeeper(configs, client)
          List("bro").foreach { sensorType =>
            var parserConfig = configs.getSensorParserConfig(sensorType)
            if (parserConfig == null) throw new IllegalStateException("Cannot find the parser configuration in zookeeper for " + sensorType + "." +
              "  Please check that it exists in zookeeper by using the 'zk_load_configs.sh -m DUMP' command.")
            parserConfigs.put(sensorType, parserConfig);
          }

          // Set up the capabilities
          var builder = new Context.Builder()
          // This syntax is confusing, but seemingly necessary to
          // a) use a reserved keyword, 'with', as a method name (i.e. use backticks)
          // b) Choose the overloaded method correctly (i.e. type the arguments)
          // c) Pass the Java lamda to Scala correctly. Theoretically it would work in 2.12 (i.e. new Capability explicitly)
          // Taken from ParserBolt.initializeStellar, with some of the indirection used to hide ZK updates and caching and such remoted.
          builder.`with`(Context.Capabilities.ZOOKEEPER_CLIENT: Enum[Capabilities], new Capability {
            override def get(): AnyRef = client
          })
          builder.`with`(Context.Capabilities.GLOBAL_CONFIG: Enum[Capabilities], new Capability {
            override def get(): AnyRef = globalConfigs
          })
          builder.`with`(Context.Capabilities.STELLAR_CONFIG: Enum[Capabilities], new Capability {
            override def get(): AnyRef = globalConfigs
          })

          // Setup Stellar
          val stellarContext: Context = builder.build
          StellarFunctions.initialize(stellarContext)

          var parserRunner = new ParserRunnerImpl(JavaConversions.setAsJavaSet(Set("bro")))
          // Do similar to above with explicit new Supplier
          var parserSupplier = new Supplier[ParserConfigurations] {
            override def get(): ParserConfigurations = configs
          }
          parserRunner.init(parserSupplier, stellarContext)

          partitionOfRecords => {
            ()
          }
            //          val connection = createNewConnection()
            //          partitionOfRecords.foreach(record => connection.send(record))
            //          connection.close()

            client.close()
        }
      }
      val lines = messages.map(_.value)
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
      wordCounts.print()

      // Start the computation
      ssc.start()
      ssc.awaitTermination()
    }
  }

}
