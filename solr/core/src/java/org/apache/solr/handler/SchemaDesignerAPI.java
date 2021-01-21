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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.UrlScheme;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.loader.DefaultSampleDocumentsLoader;
import org.apache.solr.handler.loader.SampleDocuments;
import org.apache.solr.handler.loader.SampleDocumentsLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.DefaultSchemaSuggester;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.ManagedIndexSchemaFactory;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SchemaSuggester;
import org.apache.solr.schema.TextField;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.noggit.JSONParser;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.common.StringUtils.isEmpty;
import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.common.util.Utils.fromJSONString;
import static org.apache.solr.common.util.Utils.toJavabin;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.schema.ManagedIndexSchemaFactory.DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

/**
 * All V2 APIs that have a prefix of /api/schema-designer/
 */
public class SchemaDesignerAPI {
  public static final String CONFIG_SET_PARAM = "configSet";
  public static final String COPY_FROM_PARAM = "copyFrom";
  public static final String SCHEMA_VERSION_PARAM = "schemaVersion";
  public static final String RELOAD_COLLECTIONS_PARAM = "reloadCollections";
  public static final String INDEX_TO_COLLECTION_PARAM = "indexToCollection";
  public static final String NEW_COLLECTION_PARAM = "newCollection";
  public static final String CLEANUP_TEMP_PARAM = "cleanupTemp";
  public static final String ENABLE_DYNAMIC_FIELDS_PARAM = "enableDynamicFields";
  public static final String ENABLE_FIELD_GUESSING_PARAM = "enableFieldGuessing";
  public static final String ENABLE_NESTED_DOCS_PARAM = "enableNestedDocs";
  public static final String TEMP_COLLECTION_PARAM = "tempCollection";
  public static final String DOC_ID_PARAM = "docId";
  public static final String FIELD_PARAM = "field";
  public static final String UNIQUE_KEY_FIELD_PARAM = "uniqueKeyField";
  public static final String AUTO_CREATE_FIELDS = "update.autoCreateFields";
  public static final String SOLR_CONFIG_XML = "solrconfig.xml";

  static final int MAX_SAMPLE_DOCS = 500;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DESIGNER_PREFIX = "_designer_";
  private static final Set<String> excludeConfigSetNames = new HashSet<>(Arrays.asList(DEFAULT_CONFIGSET_NAME, ".system"));
  private static final Set<String> removeFieldProps = new HashSet<>(Arrays.asList("href", "id", "copyDest"));

  private final CoreContainer coreContainer;
  private final SchemaSuggester schemaSuggester;
  private final SampleDocumentsLoader sampleDocLoader;
  private final Map<String, Integer> indexedVersion = new HashMap<>();

  public SchemaDesignerAPI(CoreContainer coreContainer) {
    this(coreContainer, SchemaDesignerAPI.newSchemaSuggester(coreContainer.getConfig()), SchemaDesignerAPI.newSampleDocumentsLoader(coreContainer.getConfig()));
  }

  SchemaDesignerAPI(CoreContainer coreContainer, SchemaSuggester schemaSuggester, SampleDocumentsLoader sampleDocLoader) {
    this.coreContainer = coreContainer;
    this.schemaSuggester = schemaSuggester;
    this.sampleDocLoader = sampleDocLoader;
  }

  public static SchemaSuggester newSchemaSuggester(NodeConfig config) {
    PluginInfo info = null; // TODO: Have NodeConfig provide PluginInfo for SchemaSuggester
    SchemaSuggester suggester;
    if (info != null) {
      suggester = config.getSolrResourceLoader().newInstance(info.className, SchemaSuggester.class);
      suggester.init(info.initArgs);
    } else {
      suggester = new DefaultSchemaSuggester();
      suggester.init(new NamedList<>());
    }
    return suggester;
  }

  public static SampleDocumentsLoader newSampleDocumentsLoader(NodeConfig config) {
    PluginInfo info = null; // TODO: Have NodeConfig provide PluginInfo for SampleDocumentsLoader
    SampleDocumentsLoader loader;
    if (info != null) {
      loader = config.getSolrResourceLoader().newInstance(info.className, SampleDocumentsLoader.class);
      loader.init(info.initArgs);
    } else {
      loader = new DefaultSampleDocumentsLoader();
      loader.init(new NamedList<>());
    }
    return loader;
  }

  @EndPoint(method = GET,
      path = "/schema-designer/info",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getInfo(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "info");
    List<String> collections = listCollectionsForConfig(configSet);
    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(CONFIG_SET_PARAM, configSet);
    String mutableId = getMutableId(configSet);
    int currentVersion = getCurrentSchemaVersion(mutableId);
    responseMap.put(SCHEMA_VERSION_PARAM, currentVersion);
    responseMap.put("collections", collections);

    List<SolrInputDocument> docs = loadSampleDocsFromBlobStore(configSet);
    responseMap.put("numDocs", docs != null ? docs.size() : -1);

    rsp.getValues().addAll(responseMap);
  }

  @EndPoint(method = POST,
      path = "/schema-designer/prep",
      permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void prepNewSchema(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "analyze");
    final String copyFrom = req.getParams().get(COPY_FROM_PARAM, DEFAULT_CONFIGSET_NAME);
    ManagedIndexSchema schema = getMutableSchemaForConfigSet(configSet, -1, copyFrom);
    String mutableId = getMutableId(configSet);
    if (!schema.persistManagedSchema(false)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to persist schema: " + mutableId);
    }

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    }

    rsp.getValues().addAll(buildResponse(configSet, schema));
  }

  @EndPoint(method = GET,
      path = "/schema-designer/file",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getFileContents(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "file");
    final String file = getRequiredParam("file", req, "file");
    String mutableId = getMutableId(configSet);
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + mutableId + "/" + file;
    SolrZkClient zkClient = zkStateReader().getZkClient();
    byte[] data = zkClient.getData(zkPath, null, null, true);
    rsp.getValues().addAll(Collections.singletonMap(file, new String(data, StandardCharsets.UTF_8)));
  }

  @EndPoint(method = POST,
      path = "/schema-designer/file",
      permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void updateFileContents(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "file");
    final String file = getRequiredParam("file", req, "file");
    String mutableId = getMutableId(configSet);
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + mutableId + "/" + file;
    SolrZkClient zkClient = zkStateReader().getZkClient();
    if (!zkClient.exists(zkPath, true)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "File '" + file + "' not found in configset: " + configSet);
    }

    ContentStream stream = extractSingleContentStream(req, true);
    byte[] data = streamAsBytes(stream.getStream());
    Exception updateFileError = null;
    if (SOLR_CONFIG_XML.equals(file)) {
      // verify the updated solrconfig.xml is valid before saving to ZK (to avoid things blowing up later)
      try {
        InMemoryResourceLoader loader = new InMemoryResourceLoader(coreContainer, mutableId, SOLR_CONFIG_XML, data);
        SolrConfig.readFromResourceLoader(loader, SOLR_CONFIG_XML, true, null);
      } catch (Exception exc) {
        updateFileError = exc;
      }
    }

    if (updateFileError == null) {
      zkClient.setData(zkPath, data, true);
    }

    Map<String, Object> response = null;
    if (updateFileError != null) {
      String errMsg = updateFileError.getMessage();
      Throwable causedBy = SolrException.getRootCause(updateFileError);
      if (causedBy != null) {
        errMsg = causedBy.getMessage();
      }
      response = new HashMap<>();
      response.put("updateFileError", errMsg);
      response.put(file, new String(data, StandardCharsets.UTF_8));
    } else {
      rebuildTempCollection(configSet, false);
      ManagedIndexSchema schema = loadLatestSchema(mutableId);
      response = buildResponse(configSet, schema);
    }
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = GET,
      path = "/schema-designer/sample",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getSampleValue(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String path = "sample";

    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, path);
    final String fieldName = getRequiredParam(FIELD_PARAM, req, path);
    final String idField = getRequiredParam(UNIQUE_KEY_FIELD_PARAM, req, path);
    String docId = req.getParams().get(DOC_ID_PARAM);

    final List<SolrInputDocument> docs = loadSampleDocsFromBlobStore(configSet);
    String textValue = null;

    if (isEmpty(docId)) {
      // no doc ID from client ... find the first doc with a non-empty string value for fieldName
      Optional<SolrInputDocument> doc = docs.stream().filter(d -> d.getFieldValue(fieldName) != null).findFirst();
      if (doc.isPresent()) {
        docId = doc.get().getFieldValue(idField).toString();
        textValue = doc.get().getFieldValue(fieldName).toString();
      }
    } else {
      final String idFilter = docId;
      Optional<SolrInputDocument> doc = docs.stream().filter(d -> idFilter.equals(d.getFieldValue(idField))).findFirst();
      if (doc.isPresent()) {
        Object fieldValue = doc.get().getFieldValue(fieldName);
        textValue = fieldValue != null ? fieldValue.toString() : "";
      }
    }

    if (textValue != null) {
      Map<String, Object> result = new HashMap<>();
      result.put(idField, docId);
      result.put(fieldName, textValue);
      // Hit the core analysis endpoint for this text
      result.put("analysis", analyzeField(getMutableId(configSet), fieldName, textValue));
      rsp.getValues().addAll(result);
    }
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> analyzeField(String mutableId, String fieldName, String fieldText) throws IOException {
    String baseUrl = getBaseUrl(mutableId);
    String fieldNameEnc = URLEncoder.encode(fieldName, StandardCharsets.UTF_8);
    String url = baseUrl + "/" + mutableId + "/analysis/field?wt=json&analysis.showmatch=true&analysis.fieldname=" + fieldNameEnc + "&analysis.fieldvalue=POST";
    HttpPost httpPost = null;
    HttpEntity entity;
    Map<String, Object> analysis = Collections.emptyMap();
    try {
      httpPost = new HttpPost(url);
      httpPost.setHeader("Content-Type", "text/plain");
      httpPost.setEntity(new ByteArrayEntity(fieldText.getBytes(StandardCharsets.UTF_8)));
      entity = cloudClient().getHttpClient().execute(httpPost).getEntity();
      Map<String, Object> response = (Map<String, Object>) fromJSONString(EntityUtils.toString(entity, StandardCharsets.UTF_8));
      if (response != null) {
        analysis = (Map<String, Object>) response.get("analysis");
      }
    } finally {
      if (httpPost != null) {
        httpPost.releaseConnection();
      }
    }
    return analysis;
  }

  @EndPoint(method = GET,
      path = "/schema-designer/collectionsForConfig",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void listCollectionsForConfig(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "collectionsForConfig");
    List<String> collections = listCollectionsForConfig(configSet);
    rsp.getValues().addAll(Collections.singletonMap("collections", collections));
  }

  protected List<String> listCollectionsForConfig(String configSet) {
    final List<String> collections = new LinkedList<>();
    Map<String, ClusterState.CollectionRef> states = zkStateReader().getClusterState().getCollectionStates();
    for (Map.Entry<String, ClusterState.CollectionRef> e : states.entrySet()) {
      final String coll = e.getKey();
      if (coll.startsWith(DESIGNER_PREFIX)) {
        continue; // ignore temp
      }

      try {
        if (configSet.equals(zkStateReader().readConfigName(coll)) && e.getValue().get() != null) {
          collections.add(coll);
        }
      } catch (Exception exc) {
        log.warn("Failed to get config name for {}", coll, exc);
      }
    }
    return collections;
  }

  @EndPoint(method = GET,
      path = "/schema-designer/configs",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void listConfigs(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.getValues().addAll(Collections.singletonMap("configSets", listConfigs()));
  }

  protected List<String> listConfigs() throws IOException {
    List<String> configsInZk = zkStateReader().getConfigManager().listConfigs();
    Set<String> configs = configsInZk.stream()
        .filter(c -> !excludeConfigSetNames.contains(c) && !c.startsWith(DESIGNER_PREFIX))
        .collect(Collectors.toSet());

    // add the in-progress but drop the _designer prefix
    configs.addAll(configsInZk.stream()
        .filter(c -> c.startsWith(DESIGNER_PREFIX))
        .map(c -> c.substring(DESIGNER_PREFIX.length()))
        .collect(Collectors.toList()));

    return configs.stream().sorted().collect(Collectors.toList());
  }

  @EndPoint(method = GET,
      path = "/schema-designer/download",
      permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void downloadConfig(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "download");
    String mutableId = getMutableId(configSet);

    // find the configset to download
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configId = mutableId;
    if (!zkClient.exists(getConfigSetZkPath(mutableId), true)) {
      if (zkClient.exists(getConfigSetZkPath(configSet), true)) {
        configId = configSet;
      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "ConfigSet " + configSet + " not found!");
      }
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ZkConfigManager cfgMgr = zkStateReader().getConfigManager();
    Path tmpDirectory = Files.createTempDirectory("schema-designer-" + configSet);
    File tmpDir = tmpDirectory.toFile();
    try {
      cfgMgr.downloadConfigDir(configId, tmpDirectory);
      try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
        zipIt(tmpDir, "", zipOut);
      }
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }

    ContentStreamBase content = new ContentStreamBase.ByteArrayStream(baos.toByteArray(), configSet + ".zip", "application/zip");
    rsp.add(RawResponseWriter.CONTENT, content);
  }

  @EndPoint(method = POST,
      path = "/schema-designer/add",
      permission = CONFIG_EDIT_PERM
  )
  @SuppressWarnings("unchecked")
  public void addSchemaObject(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);
    if (schemaVersion == -1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          SCHEMA_VERSION_PARAM + " is a required parameter for the apply action");
    }

    final String configSet = req.getParams().get(CONFIG_SET_PARAM);
    if (isEmpty(configSet)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          CONFIG_SET_PARAM + " is a required parameter for the apply action");
    }

    // an apply just copies over the temp config to the "live" location
    String mutableId = getMutableId(configSet);
    final ZkConfigManager cfgMgr = zkStateReader().getConfigManager();
    if (!cfgMgr.configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    // check the versions agree
    int currentVersion = getCurrentSchemaVersion(mutableId);
    if (currentVersion != schemaVersion) {
      throw new SolrException(SolrException.ErrorCode.CONFLICT,
          "Your schema version " + schemaVersion + " for " + configSet + " is out-of-date; current version is: " + currentVersion +
              ". Perhaps another user updated the schema before you? Please retry your request after refreshing.");
    }

    // Updated field definition is in the request body as JSON
    ContentStream stream = extractSingleContentStream(req, true);
    String contentType = stream.getContentType();
    if (isEmpty(contentType) || !contentType.toLowerCase(Locale.ROOT).contains(JSON_MIME)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected JSON in update field request!");
    }

    final Object json;
    try (Reader reader = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(reader));
    }
    log.info("Adding new schema object from JSON: {}", json);

    Map<String, Object> addJson = (Map<String, Object>) json;
    SchemaRequest.Update addAction = null;
    String action = null;
    String objectName = null;
    if (addJson.containsKey("add-field")) {
      action = "add-field";
      Map<String, Object> fieldAttrs = (Map<String, Object>) addJson.get(action);
      objectName = (String) fieldAttrs.get("name");
      addAction = new SchemaRequest.AddField(fieldAttrs);
    } else if (addJson.containsKey("add-dynamic-field")) {
      action = "add-dynamic-field";
      Map<String, Object> fieldAttrs = (Map<String, Object>) addJson.get(action);
      objectName = (String) fieldAttrs.get("name");
      addAction = new SchemaRequest.AddDynamicField(fieldAttrs);
    } else if (addJson.containsKey("add-copy-field")) {
      action = "add-copy-field";
      Map<String, Object> map = (Map<String, Object>) addJson.get("add-copy-field");
      Object dest = map.get("dest");
      List<String> destFields = null;
      if (dest instanceof String) {
        destFields = Collections.singletonList((String) dest);
      } else if (dest instanceof List) {
        destFields = (List<String>) dest;
      } else if (dest instanceof Collection) {
        Collection<String> destColl = (Collection<String>) dest;
        destFields = new ArrayList<>(destColl);
      }
      addAction = new SchemaRequest.AddCopyField((String) map.get("source"), destFields);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported action in request body! " + addJson);
    }

    SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
    if (schemaResponse.getStatus() != 0) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
    }

    ManagedIndexSchema schema = loadLatestSchema(mutableId);
    Map<String, Object> response = buildResponse(configSet, schema);
    response.put(action, objectName);
    rsp.getValues().addAll(response);
  }

  protected int rebuildTempCollection(String configSet, boolean delete) throws Exception {
    String mutableId = getMutableId(configSet);
    if (delete) {
      log.info("Deleting and re-creating existing collection {} after schema update", mutableId);
      CollectionAdminRequest.deleteCollection(mutableId).process(cloudClient());
      zkStateReader().waitForState(mutableId, 30, TimeUnit.SECONDS, Objects::isNull);
      createCollection(mutableId, mutableId);
      log.info("Deleted and re-created existing collection: {}", mutableId);
    } else {
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
      log.info("Reloaded existing collection: {}", mutableId);
    }

    RTimer timer = new RTimer();
    List<SolrInputDocument> docs = loadSampleDocsFromBlobStore(configSet);
    long numFound = indexSampleDocs(docs, mutableId);
    double tookMs = timer.getTime();
    log.debug("Indexed {} docs into temp collection {}, took {} ms", numFound, mutableId, tookMs);
    int currentVersion = getCurrentSchemaVersion(mutableId);
    indexedVersion.put(mutableId, currentVersion);
    return currentVersion;
  }

  @EndPoint(method = PUT,
      path = "/schema-designer/update",
      permission = CONFIG_EDIT_PERM
  )
  @SuppressWarnings("unchecked")
  public void updateSchemaObject(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);
    if (schemaVersion == -1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          SCHEMA_VERSION_PARAM + " is a required parameter for the update action");
    }

    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "update");

    // an apply just copies over the temp config to the "live" location
    String mutableId = getMutableId(configSet);
    final ZkConfigManager cfgMgr = zkStateReader().getConfigManager();
    if (!cfgMgr.configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    // check the versions agree
    int currentVersion = getCurrentSchemaVersion(mutableId);
    if (currentVersion != schemaVersion) {
      throw new SolrException(SolrException.ErrorCode.CONFLICT,
          "Your schema version " + schemaVersion + " for " + configSet + " is out-of-date; current version is: " + currentVersion +
              ". Perhaps another user updated the schema before you? Please retry your request after refreshing.");
    }

    log.info("Doing update on schema object: configSet={}, mutableId={}, schemaVersion={}", configSet, mutableId, schemaVersion);

    // Updated field definition is in the request body as JSON
    ContentStream stream = extractSingleContentStream(req, true);
    String contentType = stream.getContentType();
    if (isEmpty(contentType) || !contentType.toLowerCase(Locale.ROOT).contains(JSON_MIME)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected JSON in update field request!");
    }
    final Object json;
    try (Reader reader = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(reader));
    }

    log.info("Updated object in PUT: {}", json);

    Map<String, Object> updateField = (Map<String, Object>) json;
    String type = (String) updateField.get("type");
    String copyDest = (String) updateField.get("copyDest");
    Map<String, Object> fieldAttributes = updateField.entrySet().stream()
        .filter(e -> !removeFieldProps.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    boolean needsRebuild = false;
    String name = (String) fieldAttributes.get("name");
    ManagedIndexSchema schema = getMutableSchemaForConfigSet(configSet, -1, null);

    String updateType = "field";
    if (type != null) {
      // this is a field
      SchemaField schemaField = schema.getField(name);
      SimpleOrderedMap<Object> current = schemaField.getNamedPropertyValues(true);
      Map<String, Object> diff = new HashMap<>();
      for (Map.Entry<String, Object> e : fieldAttributes.entrySet()) {
        String attr = e.getKey();
        Object attrValue = e.getValue();
        if ("name".equals(attr) || "type".equals(attr)) {
          diff.put(attr, attrValue);
        } else {
          Object fromCurrent = current.get(attr);
          if (fromCurrent == null || !fromCurrent.equals(attrValue)) {
            diff.put(attr, attrValue);
          }
        }
      }

      // switch from single-valued to multi-valued requires a full rebuild
      // See SOLR-12185 ... if we're switching from single to multi-valued, then it's a big operation
      Object multiValued = fieldAttributes.get("multiValued");
      if (hasMultivalueChange(multiValued, schemaField)) {
        needsRebuild = true;
        log.warn("Need to rebuild the temp collection for {} after field {} updated to multi-valued {}", configSet, name, multiValued);
      }

      log.info("Replacing field {} with attributes: {}", name, diff);
      SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(diff);
      SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(cloudClient(), mutableId);
      if (replaceFieldResponse.getStatus() != 0) {
        Exception exc = replaceFieldResponse.getException();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exc);
      }

      schema = getMutableSchemaForConfigSet(configSet, -1, null);
      needsRebuild = applyCopyFieldUpdates(mutableId, copyDest, name, schema) || needsRebuild;
    } else {
      updateType = "type";
      FieldType fieldType = schema.getFieldType(name);

      // this is a field type
      Object multiValued = fieldAttributes.get("multiValued");
      if (multiValued == null || (Boolean.TRUE.equals(multiValued) && !fieldType.isMultiValued()) || (Boolean.FALSE.equals(multiValued) && fieldType.isMultiValued())) {
        needsRebuild = true;
        log.warn("Re-building the temp collection for {} after type {} updated to multi-valued {}", configSet, name, multiValued);
      }

      FieldTypeDefinition ftDef = new FieldTypeDefinition();
      ftDef.setAttributes(fieldAttributes);
      SchemaRequest.ReplaceFieldType replaceType = new SchemaRequest.ReplaceFieldType(ftDef);
      SchemaResponse.UpdateResponse replaceFieldResponse = replaceType.process(cloudClient(), mutableId);
      if (replaceFieldResponse.getStatus() != 0) {
        Exception exc = replaceFieldResponse.getException();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exc);
      }
    }

    // the update may have required a full rebuild of the index
    if (needsRebuild) {
      currentVersion = rebuildTempCollection(configSet, true);
    } else {
      currentVersion = getCurrentSchemaVersion(mutableId);
    }
    indexedVersion.put(mutableId, currentVersion);

    schema = loadLatestSchema(mutableId);
    Map<String, Object> response = buildResponse(configSet, schema);
    response.put(updateType, updateField);
    response.put("updateType", updateType);
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = PUT,
      path = "/schema-designer/publish",
      permission = CONFIG_EDIT_PERM
  )
  @SuppressWarnings("unchecked")
  public void publish(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);
    if (schemaVersion == -1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          SCHEMA_VERSION_PARAM + " is a required parameter for the publish action");
    }

    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "publish");

    // an apply just copies over the temp config to the "live" location
    String mutableId = getMutableId(configSet);
    final ZkConfigManager cfgMgr = zkStateReader().getConfigManager();
    if (!cfgMgr.configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    // check the versions agree
    final int currentVersion = getCurrentSchemaVersion(mutableId);
    if (currentVersion != schemaVersion) {
      throw new SolrException(SolrException.ErrorCode.CONFLICT,
          "Your schema version " + schemaVersion + " for " + configSet + " is out-of-date; current version is: " + currentVersion +
              ". Perhaps another user updated the schema before you? Please retry your request after refreshing.");
    }

    Set<String> copiedToZkPaths = new HashSet<>();
    if (cfgMgr.configExists(configSet)) {
      SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
      zkClient.zkTransfer(ZkConfigManager.CONFIGS_ZKNODE + "/" + mutableId, true,
          ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet, true, true);
    } else {
      cfgMgr.copyConfigDir(mutableId, configSet, copiedToZkPaths);
    }

    boolean reloadCollections = req.getParams().getBool(RELOAD_COLLECTIONS_PARAM, false);
    if (reloadCollections) {
      log.debug("Reloading collections after update to configSet: {}", configSet);
      List<String> collectionsForConfig = listCollectionsForConfig(configSet);
      CloudSolrClient csc = cloudClient();
      for (String next : collectionsForConfig) {
        CollectionAdminRequest.reloadCollection(next).processAsync(csc);
      }
    }

    String newCollection = req.getParams().get(NEW_COLLECTION_PARAM);
    if (!isEmpty(newCollection)) {

      if (zkStateReader().getClusterState().hasCollection(newCollection)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '" + newCollection + "' already exists!");
      }

      int numShards = req.getParams().getInt("numShards", 1);
      int rf = req.getParams().getInt("replicationFactor", 1);
      SolrResponse createCollResp = CollectionAdminRequest.createCollection(newCollection, configSet, numShards, rf).process(cloudClient());
      CollectionsHandler.waitForActiveCollection(newCollection, coreContainer, createCollResp);

      if (req.getParams().getBool(INDEX_TO_COLLECTION_PARAM, false)) {
        List<SolrInputDocument> docs = loadSampleDocsFromBlobStore(configSet);
        if (docs != null && !docs.isEmpty()) {
          RTimer timer = new RTimer();
          long numFound = indexSampleDocs(docs, newCollection);
          double tookMs = timer.getTime();
          log.debug("Indexed {} docs into collection {}, took {} ms", numFound, newCollection, tookMs);
        }
      }
    }

    boolean cleanupTemp = req.getParams().getBool(CLEANUP_TEMP_PARAM, false);
    if (cleanupTemp) {
      indexedVersion.remove(mutableId);
      CloudSolrClient cloudSolrClient = cloudClient();
      CollectionAdminRequest.deleteCollection(mutableId).process(cloudSolrClient);
      // delete the sample doc blob
      cloudSolrClient.deleteByQuery(".system", "id:" + configSet + "_sample/*", 1);
      cloudSolrClient.deleteByQuery(".system", "id:" + configSet + "_sample_raw/*", 1);
      cloudSolrClient.commit(".system", true, true);
      cfgMgr.deleteConfigDir(mutableId);
    }

    Map<String, Object> response = new HashMap<>();
    response.put(CONFIG_SET_PARAM, configSet);
    response.put(SCHEMA_VERSION_PARAM, getCurrentSchemaVersion(configSet));
    if (!isEmpty(newCollection)) {
      response.put(NEW_COLLECTION_PARAM, newCollection);
    }
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = POST,
      path = "/schema-designer/analyze",
      permission = CONFIG_EDIT_PERM
  )
  @SuppressWarnings("unchecked")
  public void analyze(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);

    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "analyze");

    // don't let the user edit the _default configSet with the designer (for now)
    if (DEFAULT_CONFIGSET_NAME.equals(configSet)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "'" + DEFAULT_CONFIGSET_NAME + "' is a reserved configSet name! Please choose a different name.");
    }

    String languages = req.getParams().get("languages");
    List<String> langs = isEmpty(languages) || "*".equals(languages) ? Collections.emptyList() : Arrays.asList(languages.split(","));

    List<SolrInputDocument> docs = null;
    String sampleDocumentsText = null;
    ContentStream stream = extractSingleContentStream(req, false);
    if (stream != null) {
      SampleDocuments sampleDocs = sampleDocLoader.load(req.getParams(), stream, MAX_SAMPLE_DOCS);
      docs = sampleDocs.parsed;
      if (!docs.isEmpty()) {
        sampleDocumentsText = storeSampleDocs(configSet, sampleDocs);
      }
    }

    if (docs == null || docs.isEmpty()) {
      // no sample docs in the request ... find in blob store (or fail if no docs previously stored)
      docs = loadSampleDocsFromBlobStore(configSet);
      if (docs == null || docs.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No sample documents provided for analyzing schema!");
      }
      // get the raw text sample docs too (if available)
      sampleDocumentsText = loadRawSampleDocsFromBlobStore(configSet);
    }

    // Get a mutable "temp" schema either from the specified copy source or configSet if it already exists.
    String defaultConfigName = DEFAULT_CONFIGSET_NAME;
    if (zkStateReader().getConfigManager().configExists(configSet)) {
      defaultConfigName = configSet;
    }
    final String copyFrom = req.getParams().get(COPY_FROM_PARAM, defaultConfigName);
    String mutableId = getMutableId(configSet);
    ManagedIndexSchema schema = getMutableSchemaForConfigSet(configSet, schemaVersion, copyFrom);

    String uniqueKeyFieldParam = req.getParams().get(UNIQUE_KEY_FIELD_PARAM);
    if (!isEmpty(uniqueKeyFieldParam)) {
      String uniqueKeyField = schema.getUniqueKeyField() != null ? schema.getUniqueKeyField().getName() : null;
      if (!uniqueKeyFieldParam.equals(uniqueKeyField)) {
        log.info("Unique key field changed from {} to {}", uniqueKeyField, uniqueKeyFieldParam);
        schema = updateUniqueKeyField(mutableId, schema, uniqueKeyFieldParam);
      }
    }

    Map<String, Object> designerInfo = getDesignerInfo(mutableId);
    Boolean enableDynamicFields = req.getParams().getBool(ENABLE_DYNAMIC_FIELDS_PARAM);
    if (enableDynamicFields != null) {
      if (!enableDynamicFields) {
        IndexSchema.DynamicField[] dynamicFields = schema.getDynamicFields();
        if (dynamicFields != null && dynamicFields.length > 0) {
          List<String> dynamicFieldNames = Arrays.asList(dynamicFields).stream().map(f -> f.getPrototype().getName()).collect(Collectors.toList());
          schema = schema.deleteDynamicFields(dynamicFieldNames);
        }
      } // TODO: go the other way?

      Boolean storedEnableDynamicFields = (Boolean) designerInfo.get("_designer." + ENABLE_DYNAMIC_FIELDS_PARAM);
      if (storedEnableDynamicFields != enableDynamicFields) {
        designerInfo.put("_designer." + ENABLE_DYNAMIC_FIELDS_PARAM, enableDynamicFields);
      }
    }

    List<String> problems = new LinkedList<>();
    schema = analyzeInputDocs(schemaSuggester.transposeDocs(docs), schema, problems, langs);

    // remove all the superfluous field types for languages we don't use
    if (!langs.isEmpty()) {
      Set<String> langSet = new HashSet<>(Arrays.asList("ws", "general"));
      langSet.addAll(langs);
      schema = pruneUnusedSchemaObjectsAndFiles(mutableId, langSet, schema);
      designerInfo.put("_designer.languages", langs);
    }

    // persist the updated schema
    if (!schema.persistManagedSchema(false)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to persist schema: " + mutableId);
    }

    Boolean enableFieldGuessing = req.getParams().getBool(ENABLE_FIELD_GUESSING_PARAM);
    if (enableFieldGuessing != null) {
      Boolean storedEnableFieldGuessing = (Boolean) designerInfo.get(AUTO_CREATE_FIELDS);
      log.info("enableFieldGuessing={}, storedEnableFieldGuessing={}", enableFieldGuessing, storedEnableFieldGuessing);

      if (enableFieldGuessing != storedEnableFieldGuessing) {
        designerInfo.put(AUTO_CREATE_FIELDS, enableFieldGuessing);
      }
    }

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    } else {
      // already created in the prep step ... reload it to pull in the updated schema
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
      log.info("Reloaded existing collection: {}", mutableId);
    }

    // remember the copyFrom param
    if (copyFrom != null && !copyFrom.equals(designerInfo.get("_designer.copyFrom"))) {
      designerInfo.put("_designer.copyFrom", copyFrom);
    }

    // nested docs
    Boolean enableNestedDocs = req.getParams().getBool(ENABLE_NESTED_DOCS_PARAM);
    if (enableNestedDocs != null) {
      Boolean stored = (Boolean) designerInfo.get("_designer." + ENABLE_NESTED_DOCS_PARAM);
      if (!enableNestedDocs.equals(stored)) {
        designerInfo.put("_designer." + ENABLE_NESTED_DOCS_PARAM, enableNestedDocs);
      }
      if (enableNestedDocs) {
        enableNestedDocsFields(schema, mutableId);
      } else {
        deleteNestedDocsFieldsIfNeeded(schema, mutableId, true);
      }
    }

    // index the sample docs using the suggested schema
    RTimer timer = new RTimer();
    long numFound = indexSampleDocs(docs, mutableId);
    double tookMs = timer.getTime();
    log.debug("Indexed {} docs into temp collection {}, took {} ms", numFound, mutableId, tookMs);

    schema = loadLatestSchema(mutableId);
    updateDesignerInfo(mutableId, designerInfo);

    Map<String, Object> response = buildResponse(configSet, schema);

    final String uniqueKeyField = schema.getUniqueKeyField().getName();
    response.put("docIds", docs.stream().map(d -> (String) d.getFieldValue(uniqueKeyField)).filter(Objects::nonNull).limit(100).collect(Collectors.toList()));
    response.put("numDocs", docs.size());

    // show the uploaded data in the sample documents window if it is small'ish
    if (sampleDocumentsText != null) {
      response.put("sampleDocuments", sampleDocumentsText);
    }

    rsp.getValues().addAll(response);
  }

  @EndPoint(method = GET,
      path = "/schema-designer/query",
      permission = CONFIG_READ_PERM
  )
  @SuppressWarnings("unchecked")
  public void query(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req, "query");
    String mutableId = getMutableId(configSet);
    if (!zkStateReader().getConfigManager().configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    }

    // TODO: only re-index if current state of test collection is not up-to-date
    int currentVersion = getCurrentSchemaVersion(mutableId);
    Integer version = indexedVersion.get(mutableId);
    if (version == null || version != currentVersion) {
      RTimer timer = new RTimer();
      log.debug("Schema for collection {} is stale ({} != {}), need to re-index sample docs", mutableId, version, currentVersion);

      List<SolrInputDocument> docs = loadSampleDocsFromBlobStore(configSet);
      long numFound = indexSampleDocs(docs, mutableId);
      double tookMs = timer.getTime();
      log.debug("Indexed {} docs into temp collection {}, took {} ms", numFound, mutableId, tookMs);
      // the version changes when you index (due to field guessing URP)
      currentVersion = getCurrentSchemaVersion(mutableId);
      indexedVersion.put(mutableId, currentVersion);
    }

    // execute the user's query against the temp collection
    SolrParams qParams = req.getParams();
    QueryResponse qr = cloudClient().query(mutableId, qParams);
    Exception exc = qr.getException();
    if (exc != null) {
      throw exc;
    }
    rsp.getValues().addAll(qr.getResponse());
  }

  @SuppressWarnings("unchecked")
  List<SolrInputDocument> loadSampleDocsFromBlobStore(final String configSet) throws IOException {
    List<SolrInputDocument> docs = null;
    String baseUrl = getBaseUrl(".system");
    String url = baseUrl + "/.system/blob/" + configSet + "_sample?wt=filestream";
    HttpGet httpGet = null;
    try {
      httpGet = new HttpGet(url);
      HttpResponse entity = cloudClient().getHttpClient().execute(httpGet);
      int statusCode = entity.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        byte[] bytes = streamAsBytes(entity.getEntity().getContent());
        if (bytes.length > 0) {
          docs = (List<SolrInputDocument>) Utils.fromJavabin(bytes);
        }
      } else if (statusCode != HttpStatus.SC_NOT_FOUND) {
        byte[] bytes = streamAsBytes(entity.getEntity().getContent());
        throw new IOException("Failed to lookup stored docs for " + configSet + " due to: " + new String(bytes, StandardCharsets.UTF_8));
      } // else not found is ok
    } finally {
      if (httpGet != null) {
        httpGet.releaseConnection();
      }
    }
    return docs != null ? docs : Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  String loadRawSampleDocsFromBlobStore(final String configSet) throws IOException {
    String sampleDocs = null;
    String baseUrl = getBaseUrl(".system");
    String url = baseUrl + "/.system/blob/" + configSet + "_sample_raw?wt=filestream";
    HttpGet httpGet = null;
    try {
      httpGet = new HttpGet(url);
      HttpResponse entity = cloudClient().getHttpClient().execute(httpGet);
      int statusCode = entity.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        byte[] bytes = streamAsBytes(entity.getEntity().getContent());
        if (bytes.length > 0) {
          sampleDocs = new String(bytes, StandardCharsets.UTF_8);
        }
      } else if (statusCode != HttpStatus.SC_NOT_FOUND) {
        byte[] bytes = streamAsBytes(entity.getEntity().getContent());
        throw new IOException("Failed to lookup raw docs for " + configSet + " due to: " + new String(bytes, StandardCharsets.UTF_8));
      } // else not found is ok
    } finally {
      if (httpGet != null) {
        httpGet.releaseConnection();
      }
    }
    return sampleDocs;
  }

  private byte[] streamAsBytes(final InputStream in) throws IOException {
    return DefaultSampleDocumentsLoader.streamAsBytes(in);
  }

  protected String storeSampleDocs(final String configSet, SampleDocuments sampleDocs) throws IOException, SolrServerException {
    postDataToBlobStore(cloudClient(), configSet + "_sample", streamAsBytes(toJavabin(sampleDocs.parsed)));

    // store raw text for UI too
    String sampleDocumentsText = sampleDocs.getSampleText();
    if (sampleDocumentsText != null) {
      postDataToBlobStore(cloudClient(), configSet + "_sample_raw", sampleDocumentsText.getBytes(StandardCharsets.UTF_8));
    } else {
      // delete the raw sample doc blob
      CloudSolrClient cloudSolrClient = cloudClient();
      cloudSolrClient.deleteByQuery(".system", "id:" + configSet + "_sample_raw/*", 1);
      cloudSolrClient.commit(".system", true, true);
    }
    return sampleDocumentsText;
  }

  @SuppressWarnings({"rawtypes"})
  protected Map postDataToBlobStore(CloudSolrClient cloudClient, String blobName, byte[] bytes) throws IOException {
    Map m = null;
    HttpPost httpPost = null;
    HttpEntity entity;
    String response = null;
    String baseUrl = getBaseUrl(".system");
    try {
      httpPost = new HttpPost(baseUrl + "/.system/blob/" + blobName);
      httpPost.setHeader("Content-Type", "application/octet-stream");
      ByteArrayEntity byteArrayEntity = new ByteArrayEntity(bytes);
      httpPost.setEntity(byteArrayEntity);
      entity = cloudClient.getHttpClient().execute(httpPost).getEntity();
      try {
        response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        m = (Map) fromJSONString(response);
      } catch (JSONParser.ParseException e) {
        log.error("$ERROR$: {}", response, e);
      }
    } finally {
      if (httpPost != null) {
        httpPost.releaseConnection();
      }
    }

    return m;
  }

  private String getBaseUrl(final String collection) {
    String baseUrl;
    Set<String> liveNodes = zkStateReader().getClusterState().getLiveNodes();
    DocCollection docColl = zkStateReader().getCollection(collection);
    if (docColl != null && !liveNodes.isEmpty()) {
      Optional<Replica> maybeActive = docColl.getReplicas().stream().filter(r -> r.isActive(liveNodes)).findAny();
      Replica active = maybeActive.orElseThrow(() -> new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          collection + " collection not active for storing sample docs in blob store!"));
      baseUrl = active.getBaseUrl();
    } else {
      // just use the baseUrl of the current node we're on
      baseUrl = UrlScheme.INSTANCE.getBaseUrlForNodeName(coreContainer.getZkController().getNodeName());
    }
    return baseUrl;
  }

  protected ManagedIndexSchema analyzeInputDocs(final Map<String, List<Object>> docs, ManagedIndexSchema schema, List<String> problems, List<String> langs) {
    // Adapt the provided schema to the sample docs
    for (String field : docs.keySet()) {
      List<Object> sampleValues = docs.getOrDefault(field, Collections.emptyList());

      // Collapse all whitespace in fields to a single underscore
      String normalizedField = field.trim().replaceAll("\\s+", "_");
      // TODO: other field name transformations as needed ... lowercase?

      if (schema.hasExplicitField(normalizedField)) {
        SchemaField existing = schema.getField(normalizedField);
        if (!schemaSuggester.matchesData(existing, sampleValues)) {
          // TODO: existing field does not match the sample data, adapt field to data
          problems.add("Existing field definition '" + normalizedField + "' does not match the sample data! field: " + existing);
        }
        continue;
      }

      Optional<SchemaField> maybeSchemaField = schemaSuggester.suggestField(normalizedField, sampleValues, schema, langs);
      if (maybeSchemaField.isPresent()) {
        schema = (ManagedIndexSchema) schema.addField(maybeSchemaField.get(), false);
      }
    }

    return schema;
  }

  String getMutableId(final String configSet) {
    return DESIGNER_PREFIX + configSet;
  }

  protected String getConfigSetZkPath(final String configSet) {
    return ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet;
  }

  protected String getManagedSchemaZkPath(final String configSet) {
    return getConfigSetZkPath(configSet) + "/" + DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME;
  }

  protected ManagedIndexSchema getMutableSchemaForConfigSet(final String configSet, final int schemaVersion, final String copyFrom) throws IOException, InterruptedException {
    // The designer works with mutable config sets stored in a "temp" znode in ZK instead of the "live" configSet
    final String mutableConfigSet = getMutableId(configSet);
    ManagedIndexSchema schema;
    schema = loadOrCreateSchema(mutableConfigSet, schemaVersion, copyFrom);
    return schema;
  }

  @SuppressWarnings("unchecked")
  protected ManagedIndexSchema loadOrCreateSchema(final String configSet, final int schemaVersion, final String copyFrom) throws IOException {

    ManagedIndexSchema schema;

    // create new from the built-in "_default" configSet
    boolean isNew = false;
    if (!zkStateReader().getConfigManager().configExists(configSet)) {
      zkStateReader().getConfigManager().copyConfigDir(copyFrom, configSet);
      log.debug("Copied '{}' to new configSet: {}", copyFrom, configSet);
      isNew = true;
    }

    SolrConfig solrConfig = loadLatestConfig(configSet);
    schema = loadLatestSchema(solrConfig);
    if (!isNew) {
      // schema is not new, so the provided version must match, otherwise, we're trying to edit dirty data
      if (schemaVersion != -1 && schemaVersion != schema.getSchemaZkVersion()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "ConfigSet '" + configSet + "' has been edited by another user! Retry your request after refreshing!");
      }
    }

    if (isNew) {
      schema = deleteNestedDocsFieldsIfNeeded(schema, configSet, false);
    }

    return schema;
  }

  protected void enableNestedDocsFields(ManagedIndexSchema schema, String mutableId) throws IOException, SolrServerException {
    if (!schema.hasExplicitField("_root_")) {
      Map<String, Object> fieldAttrs = new HashMap<>();
      fieldAttrs.put("name", "_root_");
      fieldAttrs.put("type", "string");
      fieldAttrs.put("docValues", false);
      fieldAttrs.put("indexed", true);
      fieldAttrs.put("stored", false);
      SchemaRequest.AddField addAction = new SchemaRequest.AddField(fieldAttrs);
      SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
      if (schemaResponse.getStatus() != 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add _root_ field due to: " + schemaResponse.getException());
      }
    }

    if (!schema.hasExplicitField("_nest_path_")) {
      Map<String, Object> fieldAttrs = new HashMap<>();
      fieldAttrs.put("name", "_nest_path_");
      fieldAttrs.put("type", "_nest_path_");
      SchemaRequest.AddField addAction = new SchemaRequest.AddField(fieldAttrs);
      SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
      if (schemaResponse.getStatus() != 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add _nest_path_ field due to: " + schemaResponse.getException());
      }
    }
  }

  protected ManagedIndexSchema deleteNestedDocsFieldsIfNeeded(ManagedIndexSchema schema, String mutableId, boolean persist) {
    List<String> toDelete = new LinkedList<>();
    if (schema.hasExplicitField("_root_")) {
      toDelete.add("_root_");
    }
    if (schema.hasExplicitField("_nest_path_")) {
      toDelete.add("_nest_path_");
    }
    if (!toDelete.isEmpty()) {
      schema = schema.deleteFields(toDelete);
      if (persist) {
        if (!schema.persistManagedSchema(false)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to persist schema: " + mutableId);
        }
      }
    }
    return schema;
  }

  protected Map<String, Object> getDesignerInfo(String collection) {
    return getDesignerInfo(loadLatestConfig(collection));
  }

  protected Map<String, Object> getDesignerInfo(SolrConfig solrConfig) {
    // TODO: faster to just read the overlay znode directly from ZK vs. loading SolrConfig
    Map<String, Object> map = new HashMap<>();
    ConfigOverlay overlay = solrConfig.getOverlay();
    Map<String, Object> userProps = overlay != null ? overlay.getUserProps() : null;
    if (userProps != null) {
      map.putAll(userProps);
    }
    if (!map.containsKey(AUTO_CREATE_FIELDS)) {
      map.put(AUTO_CREATE_FIELDS, true);
    }
    if (!map.containsKey("_designer." + ENABLE_DYNAMIC_FIELDS_PARAM)) {
      map.put("_designer." + ENABLE_DYNAMIC_FIELDS_PARAM, true);
    }
    if (!map.containsKey("_designer." + ENABLE_NESTED_DOCS_PARAM)) {
      map.put("_designer." + ENABLE_NESTED_DOCS_PARAM, false);
    }
    return map;
  }

  protected void updateDesignerInfo(String collection, Map<String, Object> info) throws IOException {
    Map<String, Object> stored = getDesignerInfo(collection);
    for (String prop : info.keySet()) {
      Object propValue = info.get(prop);
      if (!propValue.equals(stored.get(prop))) {
        setUserPropertyOnConfig(collection, prop, propValue);
      }
    }
  }

  protected SolrConfig loadLatestConfig(String configSet) {
    SolrResourceLoader resourceLoader = coreContainer.getResourceLoader();
    ZkSolrResourceLoader zkLoader = new ZkSolrResourceLoader(resourceLoader.getInstancePath(), configSet, resourceLoader.getClassLoader(), coreContainer.getZkController());
    return SolrConfig.readFromResourceLoader(zkLoader, SOLR_CONFIG_XML, true, null);
  }

  protected ManagedIndexSchema loadLatestSchema(String configSet) {
    return loadLatestSchema(loadLatestConfig(configSet));
  }

  protected ManagedIndexSchema loadLatestSchema(SolrConfig solrConfig) {
    ManagedIndexSchemaFactory factory = new ManagedIndexSchemaFactory();
    factory.init(new NamedList<>());
    return factory.create(DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME, solrConfig, null);
  }

  protected ContentStream extractSingleContentStream(final SolrQueryRequest req, boolean required) {
    Iterable<ContentStream> streams = req.getContentStreams();
    Iterator<ContentStream> iter = streams != null ? streams.iterator() : null;
    ContentStream stream = iter != null && iter.hasNext() ? iter.next() : null;
    if (required && stream == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No JSON content found in the request body!");

    return stream;
  }

  protected int getCurrentSchemaVersion(final String configSet) throws KeeperException, InterruptedException {
    int currentVersion = -1;
    final String path = getManagedSchemaZkPath(configSet);
    try {
      Stat stat = coreContainer.getZkController().getZkClient().exists(path, null, true);
      if (stat != null) {
        currentVersion = stat.getVersion();
      }
    } catch (KeeperException.NoNodeException notExists) {
      currentVersion = -1; // this is ok
    }
    return currentVersion;
  }

  protected void createCollection(final String collection, final String configSet) throws Exception {
    RTimer timer = new RTimer();
    SolrResponse rsp = CollectionAdminRequest.createCollection(collection, configSet, 1, 1).process(cloudClient());
    CollectionsHandler.waitForActiveCollection(collection, coreContainer, rsp);
    double tookMs = timer.getTime();
    log.info("Took {} ms to create new collection {} with configSet {}", tookMs, collection, configSet);
  }

  protected CloudSolrClient cloudClient() {
    return coreContainer.getSolrClientCache().getCloudSolrClient(coreContainer.getZkController().getZkServerAddress());
  }

  protected ZkStateReader zkStateReader() {
    return coreContainer.getZkController().getZkStateReader();
  }

  protected long indexSampleDocs(List<SolrInputDocument> docs, final String collectionName) throws Exception {
    // load sample docs from blob store
    CloudSolrClient cloudSolrClient = cloudClient();
    cloudSolrClient.deleteByQuery(collectionName, "*:*", 1);
    cloudSolrClient.optimize(collectionName, true, true, 1);

    // TODO: doc at a time will give more info
    try {
      cloudSolrClient.add(collectionName, docs, 50);
    } catch (Exception exc) {
      Throwable rootCause = SolrException.getRootCause(exc);
      String rootMsg = String.valueOf(rootCause.getMessage());
      if (rootMsg.contains("possible analysis error")) {
        log.warn("Rebuilding temp collection {} after low-level Lucene indexing issue.", collectionName, rootCause);
        // some change caused low-level lucene issues ... rebuild the collection
        CollectionAdminRequest.deleteCollection(collectionName).process(cloudClient());
        createCollection(collectionName, collectionName);
        cloudSolrClient.add(collectionName, docs, 1);
      } else {
        throw exc;
      }
    }

    cloudSolrClient.commit(collectionName, true, true, true);
    SolrQuery query = new SolrQuery("*:*");
    query.setRows(0);
    QueryResponse queryResponse = cloudSolrClient.query(collectionName, query);
    long numFound = queryResponse.getResults().getNumFound();
    if (numFound < docs.size()) {
      // wait up to 5 seconds for this to occur
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      do {
        cloudSolrClient.commit(collectionName, true, true, true);
        queryResponse = cloudSolrClient.query(collectionName, query);
        numFound = queryResponse.getResults().getNumFound();
        if (numFound >= docs.size()) {
          break;
        }
      } while (System.nanoTime() < deadline);

      if (numFound < docs.size()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Failed to index " + docs.size() + " sample docs into temp collection: " + collectionName);
      }
    }

    return numFound;
  }

  protected ManagedIndexSchema pruneUnusedSchemaObjectsAndFiles(String configSet, Set<String> languages, ManagedIndexSchema schema) throws KeeperException, InterruptedException {
    Map<String, FieldType> types = schema.getFieldTypes();
    Set<String> toRemove = new HashSet<>();
    for (Map.Entry<String, FieldType> e : types.entrySet()) {
      String name = e.getKey();
      FieldType type = e.getValue();
      if (name.startsWith("text_") && TextField.class.equals(type.getClass())) {
        String lang = name.substring("text_".length()).toLowerCase(Locale.US);
        boolean match = false;
        for (String l : languages) {
          if (lang.contains(l)) {
            match = true;
            break;
          }
        }

        if (!match) {
          toRemove.add(e.getValue().getTypeName());
        }
      }
    }

    // make sure any of the field types in the list to delete are not specifically referenced by explicit fields
    for (SchemaField field : schema.getFields().values()) {
      toRemove.remove(field.getType().getTypeName());
    }

    // remove all the lang specific stopwords too
    List<String> toRemoveFiles = new LinkedList<>();
    for (String type : toRemove) {
      TextField tf = (TextField) schema.getFieldTypeByName(type);
      Analyzer a = tf.getIndexAnalyzer();
      if (a instanceof TokenizerChain) {
        TokenizerChain chain = (TokenizerChain) a;
        TokenFilterFactory[] filters = chain.getTokenFilterFactories();
        if (filters != null) {
          for (TokenFilterFactory tff : filters) {
            if (tff instanceof StopFilterFactory) {
              StopFilterFactory sff = (StopFilterFactory) tff;
              String file = sff.getOriginalArgs().get("words");
              if (file != null) {
                toRemoveFiles.add(file);
              }
            }
          }
        }
      }
    }

    List<String> toRemoveDF = new LinkedList<>();
    IndexSchema.DynamicField[] dynamicFields = schema.getDynamicFields();
    if (dynamicFields != null) {
      for (IndexSchema.DynamicField df : dynamicFields) {
        if (toRemove.contains(df.getPrototype().getType().getTypeName())) {
          toRemoveDF.add(df.getPrototype().getName());
        }
      }
    }

    schema = schema.deleteDynamicFields(toRemoveDF);
    schema = schema.deleteFieldTypes(toRemove);

    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
    for (String file : toRemoveFiles) {
      String path = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet + "/" + file;
      try {
        zkClient.delete(path, -1, false);
      } catch (KeeperException.NoNodeException nne) {
        // no-op
      }
    }

    // catch the un-referenced (but lang specific files)
    toRemoveFiles.clear();
    String configPathInZk = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet + "/lang";
    final Set<String> langFiles = new HashSet<>();
    try {
      ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, langFiles::add);
      langFiles.remove(configPathInZk);
    } catch (KeeperException.NoNodeException nne) {
      // no-op
    }

    for (String langFile : langFiles) {
      if (langFile.startsWith("stopwords") || !langFile.endsWith(".txt")) {
        continue;
      }

      boolean langMatch = false;
      final String woExt = langFile.substring(0, langFile.length() - 4);
      for (String lang : languages) {
        if (woExt.endsWith("_" + lang)) {
          langMatch = true;
          break;
        }
      }

      if (!langMatch) {
        toRemoveFiles.add(langFile);
      }
    }

    for (String path : toRemoveFiles) {
      try {
        zkClient.delete(path, -1, false);
      } catch (KeeperException.NoNodeException nne) {
        // no-op
      }
    }

    return schema;
  }

  protected Map<String, Object> buildResponse(String configSet, ManagedIndexSchema schema) throws Exception {
    String mutableId = getMutableId(configSet);
    int currentVersion = getCurrentSchemaVersion(mutableId);
    indexedVersion.put(mutableId, currentVersion);

    // response is a map of data structures to support the schema designer
    Map<String, Object> response = new HashMap<>();

    DocCollection coll = zkStateReader().getCollection(mutableId);
    if (coll.getActiveSlicesArr().length > 0) {
      String coreName = coll.getActiveSlicesArr()[0].getLeader().getCoreName();
      response.put("core", coreName);
    }

    response.put(UNIQUE_KEY_FIELD_PARAM, schema.getUniqueKeyField().getName());

    response.put(CONFIG_SET_PARAM, configSet);
    // important: pass the designer the current schema zk version for MVCC
    response.put(SCHEMA_VERSION_PARAM, currentVersion);
    response.put(TEMP_COLLECTION_PARAM, mutableId);
    response.put("collectionsForConfig", listCollectionsForConfig(configSet));
    // Guess at a schema for each field found in the sample docs
    // Collect all fields across all docs with mapping to values
    List<SimpleOrderedMap<Object>> fields = schema.getFields().entrySet().stream()
        .map(e -> {
          SchemaField f = e.getValue();
          SimpleOrderedMap<Object> map = f.getNamedPropertyValues(true);

          // add the copy field destination field names
          List<String> copyFieldNames =
              schema.getCopyFieldsList((String) map.get("name")).stream().map(c -> c.getDestination().getName()).collect(Collectors.toList());
          map.add("copyDest", String.join(",", copyFieldNames));

          return map;
        })
        .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
        .collect(Collectors.toList());

    response.put("fields", fields);

    Map<String, Object> designerInfo = getDesignerInfo(mutableId);
    for (String key : designerInfo.keySet()) {
      Object value = designerInfo.get(key);
      if (value != null) {
        if (key.startsWith("_designer.")) {
          key = key.substring("_designer.".length());
        } else if (AUTO_CREATE_FIELDS.equals(key)) {
          key = ENABLE_FIELD_GUESSING_PARAM;
        }
        response.put(key, value);
      }
    }

    response.put("dynamicFields", Arrays.stream(schema.getDynamicFieldPrototypes())
        .map(e -> e.getNamedPropertyValues(true))
        .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
        .collect(Collectors.toList()));

    response.put("fieldTypes", schema.getFieldTypes().values().stream()
        .map(fieldType -> fieldType.getNamedPropertyValues(true))
        .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
        .collect(Collectors.toList()));

    // files
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configPathInZk = ZkConfigManager.CONFIGS_ZKNODE + "/" + mutableId;
    final Set<String> files = new HashSet<>();
    ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, files::add);
    files.remove(configPathInZk);

    final String prefix = configPathInZk + "/";
    final int prefixLen = prefix.length();
    Set<String> stripPrefix = files.stream().map(f -> f.startsWith(prefix) ? f.substring(prefixLen) : f).collect(Collectors.toSet());
    stripPrefix.remove(DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME);
    stripPrefix.remove("lang");
    response.put("files", new TreeSet<>(stripPrefix));

    // TODO: add some structure here
    //response.put("problems", problems);

    return response;
  }

  protected boolean applyCopyFieldUpdates(String mutableId, String copyDest, String fieldName, ManagedIndexSchema schema) throws IOException, SolrServerException {
    boolean updated = false;

    if (copyDest == null || copyDest.trim().isEmpty()) {
      // delete all the copy field directives for this field
      List<CopyField> copyFieldsList = schema.getCopyFieldsList(fieldName);
      if (!copyFieldsList.isEmpty()) {
        List<String> dests = copyFieldsList.stream().map(cf -> cf.getDestination().getName()).collect(Collectors.toList());
        SchemaRequest.DeleteCopyField delAction = new SchemaRequest.DeleteCopyField(fieldName, dests);
        SchemaResponse.UpdateResponse schemaResponse = delAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      }
    } else {
      Set<String> desired = new HashSet<>();
      for (String dest : copyDest.trim().split(",")) {
        desired.add(dest.trim());
      }
      Set<String> existing = schema.getCopyFieldsList(fieldName).stream().map(cf -> cf.getDestination().getName()).collect(Collectors.toSet());
      Set<String> add = Sets.difference(desired, existing);
      if (!add.isEmpty()) {
        SchemaRequest.AddCopyField addAction = new SchemaRequest.AddCopyField(fieldName, new ArrayList<>(add));
        SchemaResponse.UpdateResponse schemaResponse = addAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      } // no additions ...

      Set<String> del = Sets.difference(existing, desired);
      if (!del.isEmpty()) {
        SchemaRequest.DeleteCopyField delAction = new SchemaRequest.DeleteCopyField(fieldName, new ArrayList<>(del));
        SchemaResponse.UpdateResponse schemaResponse = delAction.process(cloudClient(), mutableId);
        if (schemaResponse.getStatus() != 0) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, schemaResponse.getException());
        }
        updated = true;
      } // no deletions ...
    }

    return updated;
  }

  protected String getRequiredParam(final String param, final SolrQueryRequest req, final String path) {
    final String paramValue = req.getParams().get(param);
    if (isEmpty(paramValue)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          param + " is a required parameter for the /schema-designer/" + path + " endpoint!");
    }
    return paramValue;
  }

  protected void zipIt(File f, String fileName, ZipOutputStream zipOut) throws IOException {
    if (f.isHidden()) {
      return;
    }

    if (f.isDirectory()) {
      String dirPrefix = "";
      if (fileName.endsWith("/")) {
        zipOut.putNextEntry(new ZipEntry(fileName));
        zipOut.closeEntry();
        dirPrefix = fileName;
      } else if (!fileName.isEmpty()) {
        dirPrefix = fileName + "/";
        zipOut.putNextEntry(new ZipEntry(dirPrefix));
        zipOut.closeEntry();
      }
      for (File child : f.listFiles()) {
        zipIt(child, dirPrefix + child.getName(), zipOut);
      }
      return;
    }

    FileInputStream fis = new FileInputStream(f);
    ZipEntry zipEntry = new ZipEntry(fileName);
    zipOut.putNextEntry(zipEntry);
    byte[] bytes = new byte[1024];
    int r;
    while ((r = fis.read(bytes)) >= 0) {
      zipOut.write(bytes, 0, r);
    }
    fis.close();
  }

  protected boolean hasMultivalueChange(Object multiValued, SchemaField schemaField) {
    return (multiValued == null ||
        (Boolean.TRUE.equals(multiValued) && !schemaField.multiValued()) ||
        (Boolean.FALSE.equals(multiValued) && schemaField.multiValued()));
  }

  protected boolean isFieldGuessingEnabled(SolrConfig solrConfig) {
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

  protected void setUserPropertyOnConfig(String collection, String propertyName, Object propertyValue) throws IOException {
    String url = getBaseUrl(collection) + "/" + collection + "/config";
    Map<String, Object> setUserProp = Collections.singletonMap("set-user-property", Collections.singletonMap(propertyName, propertyValue));
    HttpPost httpPost = null;
    try {
      httpPost = new HttpPost(url);
      httpPost.setHeader("Content-Type", "application/json");
      httpPost.setEntity(new ByteArrayEntity(JSONUtil.toJSON(setUserProp).getBytes(StandardCharsets.UTF_8)));
      HttpResponse httpResponse = cloudClient().getHttpClient().execute(httpPost);
      if (httpResponse.getStatusLine().getStatusCode() != 200) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to set user property '" + propertyName + "' to " +
            propertyValue + " for " + collection + " due to: " + EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8));
      }
    } finally {
      if (httpPost != null) {
        httpPost.releaseConnection();
      }
    }
  }

  protected ManagedIndexSchema updateUniqueKeyField(String mutableId, ManagedIndexSchema schema, String uniqueKeyField) {
    // TODO: the unique key field cannot be updated by API, so we have to edit the XML directly
    return schema;
  }

  private static class InMemoryResourceLoader extends SolrResourceLoader {
    String resource;
    byte[] data;
    ZkSolrResourceLoader delegate;

    public InMemoryResourceLoader(CoreContainer cc, String configSet, String resource, byte[] data) {
      super(cc.getResourceLoader().getInstancePath(), cc.getResourceLoader().getClassLoader());

      final SolrResourceLoader resourceLoader = cc.getResourceLoader();
      this.delegate = new ZkSolrResourceLoader(resourceLoader.getInstancePath(), configSet, resourceLoader.getClassLoader(), cc.getZkController());
      this.resource = resource;
      this.data = data;
    }

    @Override
    public InputStream openResource(String res) throws IOException {
      return (this.resource.equals(res)) ? new ByteArrayInputStream(data) : delegate.openResource(res);
    }
  }
}
