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

package org.apache.metron.beam.transform;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.metron.parsers.bro.BasicBroParser;
import org.json.simple.JSONObject;

public class MessageParser extends DoFn<KafkaRecord<byte[], byte[]>, JSONObject> {

  transient org.apache.metron.parsers.interfaces.MessageParser<?> parser;

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (parser == null) {
      parser = new BasicBroParser();
    }
    try {
      List<JSONObject> messages = (List<JSONObject>) parser.parse(c.element().getKV().getValue());
      for (JSONObject message : messages) {
        if(message == null || message.toJSONString().isEmpty()) {
          throw new IOException("well then");
        }
        c.output(message);
      }
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to parse <" + new String(c.element().getKV().getValue()) + ">due to " + e
              .getMessage(), e);
    }
  }
}