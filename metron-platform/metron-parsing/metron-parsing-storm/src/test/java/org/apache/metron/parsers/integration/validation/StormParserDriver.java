/**
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
package org.apache.metron.parsers.integration.validation;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.SerializationUtils;
import org.apache.metron.common.configuration.ParserConfigurations;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.writer.MessageWriter;
import org.apache.metron.integration.ProcessorResult;
import org.apache.metron.parsers.bolt.ParserBolt;
import org.apache.metron.parsers.bolt.WriterHandler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormParserDriver extends ParserDriver {
  private static final Logger LOG = LoggerFactory.getLogger(StormParserDriver.class);

  public static class CollectingWriter implements MessageWriter<JSONObject> {

    List<byte[]> output;
    public CollectingWriter(List<byte[]> output) {
      this.output = output;
    }

    @Override
    public void init() {

    }

    @Override
    public void write(String sensorType, WriterConfiguration configurations, Tuple tuple, JSONObject message) throws Exception {
      output.add(message.toJSONString().getBytes());
    }

    @Override
    public String getName() {
      return "collecting";
    }

    @Override
    public void close() throws Exception {
    }

    public List<byte[]> getOutput() {
      return output;
    }
  }

  private class ShimParserBolt extends ParserBolt {
    List<byte[]> output;
    List<byte[]> errors = new ArrayList<>();

    public ShimParserBolt(List<byte[]> output) {
      super(null, parserRunner, Collections.singletonMap(sensorType, new WriterHandler(new CollectingWriter(output))));
      this.output = output;
    }

    @Override
    public ParserConfigurations getConfigurations() {
      return config;
    }

    @Override
    protected void prepCache() {
    }

    @Override
    protected void handleError(String sensorType, byte[] originalMessage, Tuple tuple, Throwable ex, OutputCollector collector) {
      errors.add(originalMessage);
      LOG.error("Error parsing message: " + ex.getMessage(), ex);
    }

    @SuppressWarnings("unchecked")
    public ProcessorResult<List<byte[]>> getResults() {
      return new ProcessorResult.Builder<List<byte[]>>().withProcessErrors(errors)
                                                        .withResult(output)
                                                        .build();
    }
  }

  public StormParserDriver(String sensorType, String parserConfig, String globalConfig) throws IOException {
    super(sensorType, parserConfig, globalConfig);
  }

  public ProcessorResult<List<byte[]>> run(Iterable<byte[]> in) {
    ShimParserBolt bolt = new ShimParserBolt(new ArrayList<>());
    byte[] b = SerializationUtils.serialize(bolt);
    ShimParserBolt b2 = (ShimParserBolt) SerializationUtils.deserialize(b);
    OutputCollector collector = mock(OutputCollector.class);
    bolt.prepare(null, null, collector);
    for(byte[] record : in) {
      bolt.execute(toTuple(record));
    }
    return bolt.getResults();
  }

  public Tuple toTuple(byte[] record) {
    Tuple ret = mock(Tuple.class);
    when(ret.getStringByField("topic")).thenReturn(sensorType);
    when(ret.getBinary(eq(0))).thenReturn(record);
    return ret;
  }
}
