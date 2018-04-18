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

package org.apache.metron.solr.integration;

import static org.apache.metron.indexing.dao.metaalert.MetaAlertConstants.ALERT_FIELD;
import static org.apache.metron.indexing.dao.metaalert.MetaAlertConstants.METAALERT_FIELD;
import static org.apache.metron.indexing.dao.metaalert.MetaAlertConstants.METAALERT_TYPE;
import static org.apache.metron.indexing.dao.metaalert.MetaAlertConstants.STATUS_FIELD;
import static org.apache.metron.indexing.dao.metaalert.MetaAlertConstants.THREAT_FIELD_DEFAULT;
import static org.apache.metron.solr.dao.SolrMetaAlertDao.METAALERTS_COLLECTION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.metron.common.Constants;
import org.apache.metron.indexing.dao.metaalert.MetaAlertStatus;
import org.apache.metron.solr.integration.components.SolrComponent;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrParentChildTest {

  private static final String COLLECTION = "test";
  private static final String SOURCE_TYPE = "source.type";

  private static SolrComponent solr;

  @BeforeClass
  public static void setupBefore() throws Exception {
    // setup the client
    solr = new SolrComponent.Builder().build();
    solr.start();
  }

  @Before
  public void setup()
      throws IOException, InterruptedException, SolrServerException, KeeperException {
    solr.addCollection(METAALERTS_COLLECTION,
        "../metron-solr/src/test/resources/config/parent/conf");
    solr.addCollection(COLLECTION, "../metron-solr/src/test/resources/config/child/conf");
  }

  @AfterClass
  public static void teardown() {
    if (solr != null) {
      solr.stop();
    }
  }

  @After
  public void reset() {
    solr.reset();
  }

  @Test
  public void shouldSearchByNestedAlertAlot() throws Exception {
    for (int i = 0; i < 100; ++i) {
      System.out.println("\nRound : " + i);
      if (i != 0) {
        setup();
      }
      try {
        shouldSearchByNestedAlertMin();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      reset();
    }
  }

  @Test
  public void shouldSearchByNestedAlertMin() throws Exception {
    // Load alerts
    List<Map<String, Object>> alerts = buildAlerts(4);
    alerts.get(0).put(METAALERT_FIELD, Collections.singletonList("meta_active"));
    alerts.get(0).put("ip_src_addr", "192.168.1.1");
    alerts.get(0).put("ip_src_port", 8010);
    alerts.get(1).put(METAALERT_FIELD, Collections.singletonList("meta_active"));
    alerts.get(1).put("ip_src_addr", "192.168.1.2");
    alerts.get(1).put("ip_src_port", 8009);
    alerts.get(2).put("ip_src_addr", "192.168.1.3");
    alerts.get(2).put("ip_src_port", 8008);
    alerts.get(3).put("ip_src_addr", "192.168.1.4");
    alerts.get(3).put("ip_src_port", 8007);
    solr.addDocs(COLLECTION, alerts);

    // Load metaAlerts
    Map<String, Object> activeMetaAlert = buildMetaAlert("meta_active", MetaAlertStatus.ACTIVE,
        Optional.of(Arrays.asList(alerts.get(0), alerts.get(1))));
    Map<String, Object> inactiveMetaAlert = buildMetaAlert("meta_inactive",
        MetaAlertStatus.INACTIVE,
        Optional.of(Arrays.asList(alerts.get(2), alerts.get(3))));
    // We pass MetaAlertDao.METAALERT_TYPE, because the "_doc" gets appended automatically.
    solr.addDocs(METAALERTS_COLLECTION, Arrays.asList(activeMetaAlert, inactiveMetaAlert));

    System.out.println("Test Docs");
    SolrQuery queryAll = new SolrQuery();
    queryAll.setQuery("*:*");
    queryAll.set("collection", "test");
    queryAll.set("fl", "*,[docid]");
    QueryResponse results = solr.getSolrClient().query(queryAll);
    for (SolrDocument result : results.getResults()) {
      System.out.println(result);
    }

    System.out.println("Metaalert Docs");
    queryAll = new SolrQuery();
    queryAll.setQuery("{!child of=source.type:metaalert}");
    queryAll.set("collection", "metaalert");
    queryAll.set("fl", "*, [docid], [child parentFilter=source.type:metaalert]");
    results = solr.getSolrClient().query(queryAll);
    for (SolrDocument result : results.getResults()) {
      System.out.println("  " + result);
      if (result.hasChildDocuments()) {
        for(SolrDocument childResult : result.getChildDocuments()) {
          System.out.println("    " + childResult);
        }
      }
    }

    System.out.println("No [child]");
    queryAll = new SolrQuery();
    queryAll.setQuery(
        "+ip_src_addr:192.168.1.3 -metaalerts:[* TO *] -source.type:metaalert");
    queryAll.set("collection", "test,metaalert");
    queryAll.set("fl", "*, [docid]");
    results = solr.getSolrClient().query(queryAll);
    for (SolrDocument result : results.getResults()) {
      System.out.println("  " + result);
    }

    System.out.println("[child]");
    queryAll = new SolrQuery();
    queryAll.setQuery(
        "+ip_src_addr:192.168.1.3 -metaalerts:[* TO *] -source.type:metaalert");
    queryAll.setStart(0);
    queryAll.setRows(1);
    queryAll.set("collection", "test,metaalert");
    queryAll.set("fl", "*, [docid], [child parentFilter=source.type:metaalert]");
    results = solr.getSolrClient().query(queryAll);
    for (SolrDocument result : results.getResults()) {
      System.out.println("  " + result);
      if (result.hasChildDocuments()) {
        System.out.println("    " + result.getChildDocuments());
      }
    }
  }

  protected List<Map<String, Object>> buildAlerts(int count) {
    List<Map<String, Object>> inputData = new ArrayList<>();
    for (int i = 0; i < count; ++i) {
      final String guid = "message_" + i;
      Map<String, Object> alerts = new HashMap<>();
      alerts.put(Constants.GUID, guid);
      alerts.put(SOURCE_TYPE, COLLECTION);
      alerts.put(THREAT_FIELD_DEFAULT, (double) i);
      alerts.put("timestamp", System.currentTimeMillis());
      inputData.add(alerts);
    }
    return inputData;
  }

  protected Map<String, Object> buildMetaAlert(String guid, MetaAlertStatus status,
      Optional<List<Map<String, Object>>> alerts) {
    Map<String, Object> metaAlert = new HashMap<>();
    metaAlert.put(Constants.GUID, guid);
    metaAlert.put(SOURCE_TYPE, METAALERT_TYPE);
    metaAlert.put(STATUS_FIELD, status.getStatusString());
    if (alerts.isPresent()) {
      List<Map<String, Object>> alertsList = alerts.get();
      metaAlert.put(ALERT_FIELD, alertsList);
    }
    return metaAlert;
  }
}
