/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.nifi.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.ConfigurationType;
import org.apache.metron.common.configuration.ParserConfigurations;
import org.apache.metron.common.configuration.writer.ConfigurationStrategy;
import org.apache.metron.common.configuration.writer.ConfigurationsStrategies;
import org.apache.metron.common.zookeeper.configurations.ConfigurationsUpdater;
import org.apache.metron.common.zookeeper.configurations.Reloadable;
import org.apache.metron.zookeeper.SimpleEventListener;
import org.apache.metron.zookeeper.ZKCache;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

public class StandardMetronZkControllerService extends AbstractControllerService implements Reloadable, MetronZkControllerService {
  public static final PropertyDescriptor ZOOKEEPER_URL = new   PropertyDescriptor.Builder()
      .name("Zookeeper URL")
      .description("Comma delimited Zookeeper URLs in host:port format")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .required(true)
      .build();

  private static final List<PropertyDescriptor> serviceProperties;

  private String zookeeperUrl;
  private static final String configurationStrategy = "PARSERS";

  protected CuratorFramework client;
  protected ZKCache cache;
  private ParserConfigurations configurations;

  static{
    final List<PropertyDescriptor> props = new ArrayList<>();
    props.add(ZOOKEEPER_URL);
    serviceProperties = Collections.unmodifiableList(props);
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return serviceProperties;
  }
  @OnEnabled
  public void onConfigured(final ConfigurationContext context) throws InitializationException {
    configurations = createUpdater().defaultConfigurations();
    zookeeperUrl = context.getProperty(ZOOKEEPER_URL).getValue();
    prepCache();
  }

  @Override
  public ParserConfigurations getConfigurations() {
    return configurations;
  }

  protected ConfigurationStrategy<ParserConfigurations> getConfigurationStrategy() {
    return ConfigurationsStrategies.valueOf(configurationStrategy);
  }

  protected ConfigurationsUpdater<ParserConfigurations> createUpdater() {
    return getConfigurationStrategy().createUpdater(this, this::getConfigurations);
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
//      ConfigurationsUtils.setupStellarStatically(client);
      if (cache == null) {
        ConfigurationsUpdater<ParserConfigurations> updater = createUpdater();
        SimpleEventListener listener = new SimpleEventListener.Builder()
            .with( updater::update
                , TreeCacheEvent.Type.NODE_ADDED
                , TreeCacheEvent.Type.NODE_UPDATED
            )
            .with( updater::delete
                , TreeCacheEvent.Type.NODE_REMOVED
            )
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
      throw new RuntimeException(e);
    }
  }

  @OnDisabled
  public void shutdown() {
    cache.close();
    client.close();
  }

  @Override
  public void reloadCallback(String name, ConfigurationType type) {
  }
}
