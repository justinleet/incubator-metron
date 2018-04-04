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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.ConfigurationType;
import org.apache.metron.common.configuration.ConfigurationsUtils;
import org.apache.metron.common.configuration.EnrichmentConfigurations;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.zookeeper.configurations.ConfigurationsUpdater;
import org.apache.metron.common.zookeeper.configurations.EnrichmentUpdater;
import org.apache.metron.common.zookeeper.configurations.Reloadable;
import org.apache.metron.enrichment.adapters.stellar.StellarAdapter;
import org.apache.metron.enrichment.bolt.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.parallel.ConcurrencyContext;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;
import org.apache.metron.enrichment.parallel.ParallelEnricher;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.zookeeper.SimpleEventListener;
import org.apache.metron.zookeeper.ZKCache;
import org.json.simple.JSONObject;

public class StellarTransform extends DoFn<JSONObject, KV<String, String>> implements
    Reloadable {

  public static class Perf {

  } // used for performance logging

  public static final String STELLAR_CONTEXT_CONF = "stellarContext";

  protected Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = new HashMap<>();

  public final String zookeeperUrl;

  transient EnrichmentConfigurations configurations;
  transient Context stellarContext;
  transient StellarProcessor processor;

  transient CuratorFramework client;
  transient ZKCache cache;

  transient boolean initialized;

  protected boolean invalidateCacheOnReload = false;

  protected String sourceType = "test";

  protected ParallelEnricher enricher;
  private PerformanceLogger perfLog;

  public StellarTransform(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
  }

  public void initialize() {
    enrichmentsByType.put("stellar", new StellarAdapter());
    this.configurations = createUpdater().defaultConfigurations();
    SensorEnrichmentConfig config = getConfigurations().getSensorEnrichmentConfig(sourceType);
    if (config == null) {
//      LOG.debug("Unable to find SensorEnrichmentConfig for sourceType: {}", sourceType);
      config = new SensorEnrichmentConfig();
    }
    //This is an existing kludge for the stellar adapter to pass information along.
    //We should figure out if this can be rearchitected a bit.  This smells.
    config.getConfiguration().putIfAbsent(STELLAR_CONTEXT_CONF, stellarContext);

    stellarContext = new Context.Builder()
        .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> client)
        .with(Context.Capabilities.GLOBAL_CONFIG, () -> getConfigurations().getGlobalConfig())
        .with(Context.Capabilities.STELLAR_CONFIG, () -> getConfigurations().getGlobalConfig())
        .build();
    StellarFunctions.initialize(stellarContext);

    ConcurrencyContext.get(EnrichmentStrategies.ENRICHMENT).
        initialize(5,
            100,
            10,
            null,
            null,
           false
        );
    enricher = new ParallelEnricher(enrichmentsByType,
        ConcurrencyContext.get(EnrichmentStrategies.ENRICHMENT), false);
//    perfLog = new PerformanceLogger(() -> getConfigurations().getGlobalConfig(),
//        Perf.class.getName());
    initialized = true;
  }

  protected void prepCache() {
    try {
      if (client == null) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy);
      }
      client.start();

      //this is temporary to ensure that any validation passes.
      //The individual bolt will reinitialize stellar to dynamically pull from
      //zookeeper.
      ConfigurationsUtils.setupStellarStatically(client);
      if (cache == null) {
        ConfigurationsUpdater<EnrichmentConfigurations> updater = createUpdater();
        SimpleEventListener listener = new SimpleEventListener.Builder()
            .with(updater::update,
                TreeCacheEvent.Type.NODE_ADDED,
                TreeCacheEvent.Type.NODE_UPDATED
            )
            .with(updater::delete, TreeCacheEvent.Type.NODE_REMOVED)
            .build();
        cache = new ZKCache.Builder()
            .withClient(client)
            .withListener(listener)
            .withRoot(Constants.ZOOKEEPER_TOPOLOGY_ROOT)
            .build();
        updater.forceUpdate(client);
        cache.start();
      }
    } catch (Exception e) {
      // TODO actually log
//      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected ConfigurationsUpdater<EnrichmentConfigurations> createUpdater() {
    return new EnrichmentUpdater(this, this::getConfigurations);
  }

  public EnrichmentConfigurations getConfigurations() {
    return configurations;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (!initialized) {
      initialize();
    }
    if (client == null) {
      initialize();
      prepCache();
    }
    if (processor == null) {
      processor = new StellarProcessor();
    }
    JSONObject message = c.element();

    SensorEnrichmentConfig config = getConfigurations().getSensorEnrichmentConfig(sourceType);
    System.out.println("MESSAGE IS: " + message.toJSONString());
    System.out.println("TIMESTAMP IS: " + c.timestamp());
    System.out.println("CONFIG PRE ENRICH IS: " + config);
//    String sourceType = MessageUtils.getSensorType(message);
//    SensorEnrichmentConfig config = getConfigurations().getSensorEnrichmentConfig(sourceType);
//    ConfigHandler handler = config.getEnrichment().getEnrichmentConfigs().get("stellar");
//    getConfigurations().getGlobalConfig().get("stellar");

//    VariableResolver resolver = new MapVariableResolver(message);
    JSONObject copy = new JSONObject(message);
    try {
      ParallelEnricher.EnrichmentResult result = enricher
          .apply(copy, EnrichmentStrategies.ENRICHMENT, config, perfLog);
      copy = result.getResult();
    } catch (ExecutionException | InterruptedException e) {
      throw new IllegalStateException("Enrichment problem", e);
    }

//    JSONObject result = StellarAdapter.process(
//        new JSONObject(message),
//        handler,
//        "",
//        1000L,
//        processor,
//        resolver,
//        Context.EMPTY_CONTEXT()
//    );
//    copy.putAll(result);
    String guid = (String) message.get(Constants.GUID);
    if (guid == null) {
      guid = UUID.randomUUID().toString();
    }
    System.out.println("OUTPUTTING: " + copy.toJSONString());
    c.output(KV.of(guid, copy.toJSONString()));
  }

  @Override
  public void reloadCallback(String name, ConfigurationType type) {
    if (type == ConfigurationType.GLOBAL && enrichmentsByType != null) {
      for (EnrichmentAdapter adapter : enrichmentsByType.values()) {
        adapter.updateAdapter(getConfigurations().getGlobalConfig());
      }
    }
  }
}
