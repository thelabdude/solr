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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.SchemaDesignerAPI.getConfigSetZkPath;

public class SchemaDesignerSettingsDAO implements SchemaDesignerConstants {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrResourceLoader resourceLoader;
  private final ZkController zkController;

  SchemaDesignerSettingsDAO(SolrResourceLoader resourceLoader, ZkController zkController) {
    this.resourceLoader = resourceLoader;
    this.zkController = zkController;
  }

  SchemaDesignerSettings getSettings(String configSet) {
    ZkSolrResourceLoader zkLoader = new ZkSolrResourceLoader(resourceLoader.getInstancePath(), configSet, resourceLoader.getClassLoader(), zkController);
    return getSettings(SolrConfig.readFromResourceLoader(zkLoader, SOLR_CONFIG_XML, true, null));
  }

  SchemaDesignerSettings getSettings(SolrConfig solrConfig) {
    Map<String, Object> map = new HashMap<>();

    boolean isFieldGuessingEnabled = true;
    if (solrConfig != null) {
      ConfigOverlay overlay = solrConfig.getOverlay();
      Map<String, Object> userProps = overlay != null ? overlay.getUserProps() : null;
      if (userProps != null) {
        map.putAll(userProps);
      }
      isFieldGuessingEnabled = isFieldGuessingEnabled(solrConfig);
    }

    map.putIfAbsent(AUTO_CREATE_FIELDS, isFieldGuessingEnabled);
    map.putIfAbsent(DESIGNER_KEY + LANGUAGES_PARAM, Collections.emptyList());
    map.putIfAbsent(DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, true);
    map.putIfAbsent(DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, false);

    return new SchemaDesignerSettings(map);
  }

  boolean persistIfChanged(String configSet, SchemaDesignerSettings settings) throws IOException, KeeperException, InterruptedException {
    boolean changed = false;

    ConfigOverlay overlay = getConfigOverlay(configSet);
    Map<String, Object> storedUserProps = overlay != null ? overlay.getUserProps() : Collections.emptyMap();
    for (Map.Entry<String, Object> e : settings.toMap().entrySet()) {
      Object propValue = e.getValue();
      if (propValue != null && !propValue.equals(storedUserProps.get(e.getKey()))) {
        if (overlay == null) overlay = new ConfigOverlay(null, -1);
        overlay = overlay.setUserProperty(e.getKey(), propValue);
        changed = true;
      }
    }

    if (changed) {
      ZkSolrResourceLoader zkLoader =
          new ZkSolrResourceLoader(resourceLoader.getInstancePath(), configSet, resourceLoader.getClassLoader(), zkController);
      ZkController.persistConfigResourceToZooKeeper(zkLoader, overlay.getZnodeVersion(),
          ConfigOverlay.RESOURCE_NAME, overlay.toByteArray(), true);
    }

    return changed;
  }

  boolean isDesignerDisabled(String configSet) {
    // filter out any configs that don't want to be edited by the Schema Designer
    // this allows users to lock down specific configs from being edited by the designer
    boolean disabled;
    try {
      ConfigOverlay overlay = getConfigOverlay(configSet);
      Map<String, Object> userProps = overlay != null ? overlay.getUserProps() : Collections.emptyMap();
      disabled = (boolean) userProps.getOrDefault(DESIGNER_KEY + DISABLED, false);
    } catch (Exception exc) {
      log.error("Failed to load configoverlay.json for configset {}", configSet, exc);
      disabled = true; // error on the side of caution here
    }
    return disabled;
  }

  @SuppressWarnings("unchecked")
  private ConfigOverlay getConfigOverlay(String config) throws IOException, KeeperException, InterruptedException {
    ConfigOverlay overlay = null;
    String path = getConfigSetZkPath(config, CONFIGOVERLAY_JSON);
    byte[] data = null;
    Stat stat = new Stat();
    try {
      data = zkController.getZkStateReader().getZkClient().getData(path, null, stat, true);
    } catch (KeeperException.NoNodeException nne) {
      // ignore
    }
    if (data != null && data.length > 0) {
      Map<String, Object> json =
          (Map<String, Object>) ObjectBuilder.getVal(new JSONParser(new String(data, StandardCharsets.UTF_8)));
      overlay = new ConfigOverlay(json, stat.getVersion());
    }
    return overlay;
  }

  private boolean isFieldGuessingEnabled(SolrConfig solrConfig) {
    boolean hasPlugin = false;
    List<PluginInfo> plugins = solrConfig.getPluginInfos(UpdateRequestProcessorChain.class.getName());
    if (plugins != null) {
      for (PluginInfo next : plugins) {
        if ("add-unknown-fields-to-the-schema".equals(next.name)) {
          hasPlugin = true;
          break;
        }
      }
    }

    boolean isEnabled = hasPlugin;
    if (hasPlugin) {
      ConfigOverlay overlay = solrConfig.getOverlay();
      if (overlay != null) {
        Map<String, Object> userProps = overlay.getUserProps();
        if (userProps != null) {
          Boolean autoCreateFields = (Boolean) userProps.get(AUTO_CREATE_FIELDS);
          if (autoCreateFields != null) {
            isEnabled = autoCreateFields;
          }
        }
      }
    }
    return isEnabled;
  }
}
