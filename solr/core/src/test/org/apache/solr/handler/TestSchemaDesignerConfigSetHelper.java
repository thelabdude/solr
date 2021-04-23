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

package org.apache.solr.handler;

import java.io.File;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.handler.SchemaDesignerAPI.getMutableId;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;

public class TestSchemaDesignerConfigSetHelper extends SolrCloudTestCase {

  private CoreContainer cc;
  private SchemaDesignerConfigSetHelper helper;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1).addConfig("_default", new File(ExternalPaths.DEFAULT_CONFIGSET).toPath()).configure();
    // SchemaDesignerConfigSetHelper depends on the blob store
    CollectionAdminRequest.createCollection(".system", 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(".system", 1, 1);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      cluster.deleteAllCollections();
      cluster.deleteAllConfigSets();
    }
  }

  @Before
  public void setupTest() {
    assumeWorkingMockito();
    assertNotNull(cluster);
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    assertNotNull(cc);
    helper = new SchemaDesignerConfigSetHelper(cc, SchemaDesignerAPI.newSchemaSuggester(cc.getConfig()),
        new SchemaDesignerSettingsDAO(cc.getResourceLoader(), cc.getZkController()));
  }

  @Test
  public void testBasics() throws Exception {
    String configSet = "testHelper";
    String mutableId = getMutableId(configSet);
    // create our test config by copying from _default
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    SolrConfig solrConfig = helper.loadSolrConfig(mutableId);
    ManagedIndexSchema schema = helper.loadLatestSchema(solrConfig);
    assertNotNull(schema);

    Map<String, Boolean> configs = helper.listEnabledConfigs();
    assertNotNull(configs);
  }
}
