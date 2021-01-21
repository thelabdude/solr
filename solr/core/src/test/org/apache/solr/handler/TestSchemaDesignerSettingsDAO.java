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
import java.util.Collections;
import java.util.Map;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.handler.SchemaDesignerAPI.AUTO_CREATE_FIELDS;
import static org.apache.solr.handler.SchemaDesignerAPI.COPY_FROM_PARAM;
import static org.apache.solr.handler.SchemaDesignerAPI.DESIGNER_KEY;
import static org.apache.solr.handler.SchemaDesignerAPI.DISABLED;
import static org.apache.solr.handler.SchemaDesignerAPI.ENABLE_DYNAMIC_FIELDS_PARAM;
import static org.apache.solr.handler.SchemaDesignerAPI.ENABLE_NESTED_DOCS_PARAM;
import static org.apache.solr.handler.SchemaDesignerAPI.LANGUAGES_PARAM;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;

public class TestSchemaDesignerSettingsDAO extends SolrCloudTestCase {

  private CoreContainer cc;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1).addConfig("_default", new File(ExternalPaths.DEFAULT_CONFIGSET).toPath()).configure();
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
    assertNotNull(cluster);
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    assertNotNull(cc);
  }

  @Test
  public void testDAO() throws Exception {
    String collection = "testDAO";
    String configSet = DEFAULT_CONFIGSET_NAME;

    SolrResponse rsp =
        CollectionAdminRequest.createCollection(collection, configSet, 1, 1).process(cluster.getSolrClient());
    CollectionsHandler.waitForActiveCollection(collection, cc, rsp);

    SchemaDesignerSettingsDAO dao = new SchemaDesignerSettingsDAO(cc.getResourceLoader(), cc.getZkController());
    Map<String, Object> settings = dao.getSettings(configSet);
    assertNotNull(settings);

    Map<String, Object> expSettings = makeMap(
        DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, true,
        AUTO_CREATE_FIELDS, true,
        DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, false,
        DESIGNER_KEY + LANGUAGES_PARAM, Collections.emptyList());

    assertDesignerSettings(expSettings, settings);

    settings.put(DESIGNER_KEY + DISABLED, false);
    settings.put(DESIGNER_KEY + COPY_FROM_PARAM, "foo");

    assertTrue("updated settings should have changed in ZK", dao.persistIfChanged(configSet, settings));

    settings = dao.getSettings(configSet);
    assertNotNull(settings);

    expSettings = makeMap(
        DESIGNER_KEY + DISABLED, false,
        DESIGNER_KEY + COPY_FROM_PARAM, "foo",
        DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, true,
        AUTO_CREATE_FIELDS, true,
        DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, false,
        DESIGNER_KEY + LANGUAGES_PARAM, Collections.emptyList());
    assertDesignerSettings(expSettings, settings);
    assertFalse("should not be disabled", dao.isDesignerDisabled(configSet));

    settings.put(DESIGNER_KEY + DISABLED, true);
    settings.put(DESIGNER_KEY + COPY_FROM_PARAM, "bar");
    settings.put(DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, false);
    settings.put(DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, true);
    settings.put(AUTO_CREATE_FIELDS, false);
    settings.put(DESIGNER_KEY + LANGUAGES_PARAM, Collections.singletonList("en"));

    assertTrue("updated settings should have changed in ZK", dao.persistIfChanged(configSet, settings));
    settings = dao.getSettings(configSet);
    assertNotNull(settings);

    expSettings = makeMap(
        DESIGNER_KEY + DISABLED, true,
        DESIGNER_KEY + COPY_FROM_PARAM, "bar",
        DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, false,
        AUTO_CREATE_FIELDS, false,
        DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, true,
        DESIGNER_KEY + LANGUAGES_PARAM, Collections.singletonList("en"));
    assertDesignerSettings(expSettings, settings);
    assertTrue("should be disabled", dao.isDesignerDisabled(configSet));
  }

  protected void assertDesignerSettings(Map<String, Object> expected, Map<String, Object> actual) {
    for (String expKey : expected.keySet()) {
      Object expValue = expected.get(expKey);
      Object actValue = actual.get(expKey);
      assertEquals("Value for designer setting '" + expKey + "' not match expected!", expValue, actValue);
    }
  }
}
