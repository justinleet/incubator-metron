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
package org.apache.metron.solr.dao;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import org.apache.metron.indexing.dao.AccessConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SolrDaoTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private SolrClient client;
  private SolrSearchDao solrSearchDao;
  private SolrUpdateDao solrUpdateDao;
  private SolrRetrieveLatestDao solrRetrieveLatestDao;
  private SolrColumnMetadataDao solrColumnMetadataDao;
  private SolrDao solrDao;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    client = mock(SolrClient.class);
    solrSearchDao = mock(SolrSearchDao.class);
    solrUpdateDao = mock(SolrUpdateDao.class);
    solrRetrieveLatestDao = mock(SolrRetrieveLatestDao.class);
    solrColumnMetadataDao = mock(SolrColumnMetadataDao.class);
  }

  @Test
  public void initShouldEnableKerberos() {
    AccessConfig accessConfig = new AccessConfig();

    solrDao = spy(new SolrDao(
        client,
        accessConfig,
        solrSearchDao,
        solrUpdateDao,
        solrRetrieveLatestDao,
        solrColumnMetadataDao));
    doNothing().when(solrDao).enableKerberos();

    solrDao.init(accessConfig);

    verify(solrDao, times(0)).enableKerberos();

    accessConfig.setKerberosEnabled(true);

    solrDao.init(accessConfig);
    verify(solrDao).enableKerberos();
  }
}
