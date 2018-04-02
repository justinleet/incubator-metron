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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.enrichment.EnrichmentConfig;
import org.apache.metron.common.configuration.enrichment.handler.ConfigHandler;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.enrichment.adapters.stellar.StellarAdapter;
import org.apache.metron.enrichment.bolt.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.stellar.dsl.VariableResolver;
import org.json.simple.JSONObject;

public class StellarTransform extends DoFn<JSONObject, KV<String, String>> {

  /**
   {
     "fieldMap": {
       "stellar" : {
         "config" : {
           "stmt1" : "TO_UPPER(source.type)",
           "stmt2" : "TO_LOWER(iflags)"
         }
       }
     }
   }
   */
  @Multiline
  public static String defaultStellarConfig_map;

  private Context stellarContext = Context.EMPTY_CONTEXT();
  protected Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = new HashMap<>();
  transient StellarProcessor processor;


  {
    enrichmentsByType.put("ENRICHMENT", new StellarAdapter());
    StellarFunctions.initialize(stellarContext);
  }


  @ProcessElement
  public void processElement(ProcessContext c) {
    if(processor == null) {
      processor = new StellarProcessor();
    }
    JSONObject message = c.element();
    EnrichmentConfig enrichmentConfig = null;

    try {
      enrichmentConfig = JSONUtils.INSTANCE.load(defaultStellarConfig_map, EnrichmentConfig.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ConfigHandler handler = enrichmentConfig.getEnrichmentConfigs().get("stellar");

    VariableResolver resolver = new MapVariableResolver(message);
    JSONObject copy = new JSONObject(message);
    JSONObject result = StellarAdapter.process(
        new JSONObject(message),
        handler,
        "",
        1000L,
        processor,
        resolver,
        Context.EMPTY_CONTEXT()
    );
    copy.putAll(result);
    String guid = (String) message.get(Constants.GUID);
    if(guid == null) {
      guid = UUID.randomUUID().toString();
    }
    c.output(KV.of(guid, copy.toJSONString()));
  }
}
