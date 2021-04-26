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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.loader.DefaultSampleDocumentsLoader;
import org.apache.solr.handler.loader.SampleDocuments;
import org.apache.solr.handler.loader.SampleDocumentsLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DefaultSchemaSuggester;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SchemaSuggester;
import org.apache.solr.schema.StrField;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.KeeperException;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.common.StringUtils.isEmpty;
import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.common.util.Utils.makeMap;
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
  public static final String PUBLISHED_VERSION = "publishedVersion";
  public static final String DISABLE_DESIGNER_PARAM = "disableDesigner";
  public static final String DISABLED = "disabled";
  public static final String DOC_ID_PARAM = "docId";
  public static final String FIELD_PARAM = "field";
  public static final String UNIQUE_KEY_FIELD_PARAM = "uniqueKeyField";
  public static final String AUTO_CREATE_FIELDS = "update.autoCreateFields";
  public static final String SOLR_CONFIG_XML = "solrconfig.xml";
  public static final String DESIGNER_KEY = "_designer.";
  public static final String LANGUAGES_PARAM = "languages";
  public static final String CONFIGOVERLAY_JSON = "configoverlay.json";

  static final String DESIGNER_PREFIX = "._designer_";
  static final int MAX_SAMPLE_DOCS = 1000;

  private static final String UPDATE_ERROR = "updateError";
  private static final String ERROR_DETAILS = "errorDetails";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final SchemaSuggester schemaSuggester;
  private final SampleDocumentsLoader sampleDocLoader;
  private final SchemaDesignerSettingsDAO settingsDAO;
  private final SchemaDesignerConfigSetHelper configSetHelper;
  private final Map<String, Integer> indexedVersion = new ConcurrentHashMap<>();

  public SchemaDesignerAPI(CoreContainer coreContainer) {
    this(coreContainer, SchemaDesignerAPI.newSchemaSuggester(coreContainer.getConfig()), SchemaDesignerAPI.newSampleDocumentsLoader(coreContainer.getConfig()));
  }

  SchemaDesignerAPI(CoreContainer coreContainer, SchemaSuggester schemaSuggester, SampleDocumentsLoader sampleDocLoader) {
    this.coreContainer = coreContainer;
    this.schemaSuggester = schemaSuggester;
    this.sampleDocLoader = sampleDocLoader;
    this.settingsDAO = new SchemaDesignerSettingsDAO(coreContainer.getResourceLoader(), coreContainer.getZkController());
    this.configSetHelper = new SchemaDesignerConfigSetHelper(this.coreContainer, this.schemaSuggester, this.settingsDAO);
  }

  public static SchemaSuggester newSchemaSuggester(NodeConfig config) {
    DefaultSchemaSuggester suggester = new DefaultSchemaSuggester();
    suggester.init(new NamedList<>());
    return suggester;
  }

  public static SampleDocumentsLoader newSampleDocumentsLoader(NodeConfig config) {
    SampleDocumentsLoader loader = new DefaultSampleDocumentsLoader();
    loader.init(new NamedList<>());
    return loader;
  }

  static String getConfigSetZkPath(final String configSet) {
    return getConfigSetZkPath(configSet, null);
  }

  static String getConfigSetZkPath(final String configSet, final String childNode) {
    String path = ZkConfigSetService.CONFIGS_ZKNODE + "/" + configSet;
    if (childNode != null) {
      path += "/" + childNode;
    }
    return path;
  }

  static String getMutableId(final String configSet) {
    return DESIGNER_PREFIX + configSet;
  }

  @EndPoint(method = GET, path = "/schema-designer/info", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getInfo(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);

    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(CONFIG_SET_PARAM, configSet);
    boolean exists = configExists(configSet);
    responseMap.put("published", exists);

    // mutable config may not exist yet as this is just an info check to gather some basic info the UI needs
    String mutableId = getMutableId(configSet);
    Map<String, Object> settings;
    if (configExists(mutableId)) {
      // if there's a mutable config, prefer the settings from that first but fallback to the original if not found
      settings = settingsDAO.getSettings(configSetHelper.loadSolrConfig(mutableId));
    } else {
      settings = settingsDAO.getSettings(exists ? configSetHelper.loadSolrConfig(configSet) : null);
    }
    addSettingsToResponse(settings, responseMap);

    responseMap.put(SCHEMA_VERSION_PARAM, configSetHelper.getCurrentSchemaVersion(mutableId));
    responseMap.put("collections", exists ? configSetHelper.listCollectionsForConfig(configSet) : Collections.emptyList());

    // don't fail if loading sample docs fails
    try {
      responseMap.put("numDocs", configSetHelper.loadSampleDocsFromBlobStore(configSet).size());
    } catch (Exception exc) {
      log.warn("Failed to load sample docs from blob store for {}", configSet, exc);
    }

    rsp.getValues().addAll(responseMap);
  }

  @EndPoint(method = POST, path = "/schema-designer/prep", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void prepNewSchema(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String copyFrom = req.getParams().get(COPY_FROM_PARAM, DEFAULT_CONFIGSET_NAME);

    final Map<String, Object> settings = new HashMap<>();
    ManagedIndexSchema schema = getMutableSchemaForConfigSet(configSet, -1, copyFrom, settings);
    String mutableId = getMutableId(configSet);

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      indexedVersion.remove(mutableId);
      configSetHelper.createCollection(mutableId, mutableId);
    }

    settingsDAO.persistIfChanged(mutableId, settings);

    rsp.getValues().addAll(buildResponse(configSet, schema, settings, null));
  }

  @EndPoint(method = PUT, path = "/schema-designer/cleanup", permission = CONFIG_EDIT_PERM)
  public void cleanupTemp(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, SolrServerException {
    cleanupTemp(getRequiredParam(CONFIG_SET_PARAM, req));
  }

  @EndPoint(method = GET, path = "/schema-designer/file", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getFileContents(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String file = getRequiredParam("file", req);
    byte[] data = zkStateReader().getZkClient().getData(getConfigSetZkPath(getMutableId(configSet), file), null, null, true);
    rsp.getValues().addAll(Collections.singletonMap(file, new String(data, StandardCharsets.UTF_8)));
  }

  @EndPoint(method = POST, path = "/schema-designer/file", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void updateFileContents(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String file = getRequiredParam("file", req);
    String mutableId = getMutableId(configSet);
    String zkPath = getConfigSetZkPath(getMutableId(configSet), file);
    SolrZkClient zkClient = zkStateReader().getZkClient();
    if (!zkClient.exists(zkPath, true)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "File '" + file + "' not found in configSet: " + configSet);
    }

    byte[] data = streamAsBytes(extractSingleContentStream(req, true).getStream());
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

    if (updateFileError != null) {
      // solrconfig.xml update failed, but haven't impacted the configSet yet, so just return the error directly
      Throwable causedBy = SolrException.getRootCause(updateFileError);
      Map<String, Object> response = new HashMap<>();
      response.put("updateFileError", causedBy.getMessage());
      response.put(file, new String(data, StandardCharsets.UTF_8));
      rsp.getValues().addAll(response);
      return;
    }

    // apply the update and reload the temp collection / re-index sample docs
    zkClient.setData(zkPath, data, true);
    configSetHelper.reloadTempCollection(mutableId, false);

    Map<String, Object> settings = new HashMap<>();
    ManagedIndexSchema schema = loadLatestSchema(mutableId, settings);
    Map<Object, Throwable> errorsDuringIndexing = null;
    SolrException solrExc = null;
    List<SolrInputDocument> docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);
    if (!docs.isEmpty()) {
      try {
        errorsDuringIndexing = indexSampleDocsWithRebuildOnAnalysisError(schema.getUniqueKeyField().getName(), docs, mutableId, true);
      } catch (SolrException exc) {
        solrExc = exc;
      }
    }

    Map<String, Object> response = buildResponse(configSet, schema, settings, docs);

    addErrorToResponse(mutableId, solrExc, errorsDuringIndexing, response,
        "Failed to re-index sample documents after update to the " + file + " file");

    rsp.getValues().addAll(response);
  }

  @EndPoint(method = GET, path = "/schema-designer/sample", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void getSampleValue(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String fieldName = getRequiredParam(FIELD_PARAM, req);
    final String idField = getRequiredParam(UNIQUE_KEY_FIELD_PARAM, req);
    String docId = req.getParams().get(DOC_ID_PARAM);

    final List<SolrInputDocument> docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);
    String textValue = null;
    if (isEmpty(docId)) {
      // no doc ID from client ... find the first doc with a non-empty string value for fieldName
      Optional<SolrInputDocument> doc = docs.stream()
          .filter(d -> d.getField(fieldName) != null && d.getField(fieldName).getFirstValue() != null && !d.getField(fieldName).getFirstValue().toString().isEmpty())
          .findFirst();
      if (doc.isPresent()) {
        docId = doc.get().getFieldValue(idField).toString();
        textValue = doc.get().getField(fieldName).getFirstValue().toString();
      }
    } else {
      final String idFilter = docId;
      Optional<SolrInputDocument> doc = docs.stream().filter(d -> idFilter.equals(d.getFieldValue(idField))).findFirst();
      if (doc.isPresent()) {
        SolrInputField field = doc.get().getField(fieldName);
        textValue = field != null && field.getFirstValue() != null ? field.getFirstValue().toString() : "";
      }
    }

    if (textValue != null) {
      Map<String, Object> analysis = configSetHelper.analyzeField(getMutableId(configSet), fieldName, textValue);
      rsp.getValues().addAll(makeMap(idField, docId, fieldName, textValue, "analysis", analysis));
    }
  }

  @EndPoint(method = GET, path = "/schema-designer/collectionsForConfig", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void listCollectionsForConfig(SolrQueryRequest req, SolrQueryResponse rsp) {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    rsp.getValues().addAll(Collections.singletonMap("collections", configSetHelper.listCollectionsForConfig(configSet)));
  }

  @EndPoint(method = GET, path = "/schema-designer/configs", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void listConfigs(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.getValues().addAll(Collections.singletonMap("configSets", configSetHelper.listEnabledConfigs()));
  }

  @EndPoint(method = GET, path = "/schema-designer/download", permission = CONFIG_READ_PERM)
  public void downloadConfig(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    String mutableId = getMutableId(configSet);

    // find the configSet to download
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configId = mutableId;
    if (!zkClient.exists(getConfigSetZkPath(mutableId, null), true)) {
      if (zkClient.exists(getConfigSetZkPath(configSet, null), true)) {
        configId = configSet;
      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "ConfigSet " + configSet + " not found!");
      }
    }

    byte[] configSetZip = configSetHelper.downloadAndZipConfigSet(configId);
    ContentStreamBase content =
        new ContentStreamBase.ByteArrayStream(configSetZip, configSet + ".zip", "application/zip");
    rsp.add(RawResponseWriter.CONTENT, content);
  }

  @EndPoint(method = POST, path = "/schema-designer/add", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void addSchemaObject(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String mutableId = checkMutable(configSet, req);

    Map<String, Object> addJson = readJsonFromRequest(req);
    log.info("Adding new schema object from JSON: {}", addJson);

    String objectName = configSetHelper.addSchemaObject(configSet, addJson);
    String action = addJson.keySet().iterator().next();

    Map<String, Object> settings = new HashMap<>();
    ManagedIndexSchema schema = loadLatestSchema(mutableId, settings);
    Map<String, Object> response = buildResponse(configSet, schema, settings, configSetHelper.loadSampleDocsFromBlobStore(configSet));
    response.put(action, objectName);
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = PUT, path = "/schema-designer/update", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void updateSchemaObject(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String mutableId = checkMutable(configSet, req);

    // Updated field definition is in the request body as JSON
    Map<String, Object> updateField = readJsonFromRequest(req);
    String name = (String) updateField.get("name");
    if (isEmpty(name)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid update request! JSON payload is missing the required name property: " + updateField);
    }
    log.info("Updating schema object: configSet={}, mutableId={}, name={}, JSON={}", configSet, mutableId, name, updateField);

    Map<String, Object> settings = new HashMap<>();
    ManagedIndexSchema schemaBeforeUpdate = getMutableSchemaForConfigSet(configSet, -1, null, settings);

    Map<String, Object> updateResult = configSetHelper.updateSchemaObject(configSet, updateField, schemaBeforeUpdate);
    SolrException solrExc = (SolrException) updateResult.get("solrExc");
    String updateError = (String) updateResult.get(UPDATE_ERROR);
    String updateType = (String) updateResult.get("updateType");
    boolean needsRebuild = (boolean) updateResult.get("rebuild");

    // re-index the docs if no error to this point
    final ManagedIndexSchema schema = loadLatestSchema(mutableId, settings);
    List<SolrInputDocument> docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (solrExc == null && !docs.isEmpty()) {
      try {
        errorsDuringIndexing = indexSampleDocsWithRebuildOnAnalysisError(schema.getUniqueKeyField().getName(), docs, mutableId, false);
      } catch (SolrException exc) {
        solrExc = exc;
        updateError = "Failed to re-index sample documents after update to the " + name + " " + updateType + " due to: " + solrExc.getMessage();
      }
    }

    Map<String, Object> response = buildResponse(configSet, schema, settings, docs);
    response.put("updateType", updateType);
    if (FIELD_PARAM.equals(updateType)) {
      response.put(updateType, fieldToMap(schema.getField(name), schema));
    } else if ("type".equals(updateType)) {
      response.put(updateType, schema.getFieldTypeByName(name).getNamedPropertyValues(true));
    }

    addErrorToResponse(mutableId, solrExc, errorsDuringIndexing, response, updateError);

    response.put("rebuild", needsRebuild);
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = PUT, path = "/schema-designer/publish", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void publish(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    final String mutableId = checkMutable(configSet, req);

    Map<String, Object> settings = settingsDAO.getSettings(configSetHelper.loadSolrConfig(mutableId));
    final Number publishedVersion = (Number) settings.get(DESIGNER_KEY + PUBLISHED_VERSION);
    if (publishedVersion != null) {
      int currentVersionOfSrc = configSetHelper.getCurrentSchemaVersion(configSet);
      if (publishedVersion.intValue() != currentVersionOfSrc) {
        throw new SolrException(SolrException.ErrorCode.CONFLICT,
            "Version mismatch for " + configSet + "! Expected version " + publishedVersion.intValue() + " but current is " + currentVersionOfSrc +
                "; another user may have changed the published schema while you were making edits. " +
                "Publishing your changes would result in losing the edits from the other user.");
      }
    }

    String newCollection = req.getParams().get(NEW_COLLECTION_PARAM);
    if (!isEmpty(newCollection)) {
      if (zkStateReader().getClusterState().hasCollection(newCollection)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '" + newCollection + "' already exists!");
      }
    }

    if (configExists(configSet)) {
      SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
      zkClient.zkTransfer(getConfigSetZkPath(mutableId), true, getConfigSetZkPath(configSet), true, true);
    } else {
      copyConfig(mutableId, configSet);
    }

    boolean reloadCollections = req.getParams().getBool(RELOAD_COLLECTIONS_PARAM, false);
    if (reloadCollections) {
      log.debug("Reloading collections after update to configSet: {}", configSet);
      List<String> collectionsForConfig = configSetHelper.listCollectionsForConfig(configSet);
      CloudSolrClient csc = cloudClient();
      for (String next : collectionsForConfig) {
        CollectionAdminRequest.reloadCollection(next).processAsync(csc);
      }
    }

    // create new collection
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (!isEmpty(newCollection)) {
      int numShards = req.getParams().getInt("numShards", 1);
      int rf = req.getParams().getInt("replicationFactor", 1);
      SolrResponse createCollResp = CollectionAdminRequest.createCollection(newCollection, configSet, numShards, rf).process(cloudClient());
      CollectionsHandler.waitForActiveCollection(newCollection, coreContainer, createCollResp);

      if (req.getParams().getBool(INDEX_TO_COLLECTION_PARAM, false)) {
        List<SolrInputDocument> docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);
        if (!docs.isEmpty()) {
          ManagedIndexSchema schema = loadLatestSchema(mutableId, null);
          errorsDuringIndexing = indexSampleDocs(schema.getUniqueKeyField().getName(), docs, newCollection, true);
        }
      }
    }

    boolean cleanupTemp = req.getParams().getBool(CLEANUP_TEMP_PARAM, false);
    if (cleanupTemp) {
      cleanupTemp(configSet);
    }

    settings.put(DESIGNER_KEY + DISABLED, req.getParams().getBool(DISABLE_DESIGNER_PARAM, false));
    settingsDAO.persistIfChanged(configSet, settings);

    Map<String, Object> response = new HashMap<>();
    response.put(CONFIG_SET_PARAM, configSet);
    response.put(SCHEMA_VERSION_PARAM, configSetHelper.getCurrentSchemaVersion(configSet));
    if (!isEmpty(newCollection)) {
      response.put(NEW_COLLECTION_PARAM, newCollection);
    }

    addErrorToResponse(newCollection, null, errorsDuringIndexing, response, null);

    rsp.getValues().addAll(response);
  }

  @EndPoint(method = POST, path = "/schema-designer/analyze", permission = CONFIG_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void analyze(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);

    // don't let the user edit the _default configSet with the designer (for now)
    if (DEFAULT_CONFIGSET_NAME.equals(configSet)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "'" + DEFAULT_CONFIGSET_NAME + "' is a reserved configSet name! Please choose a different name.");
    }

    // Get the sample documents to analyze, preferring those in the request but falling back to previously stored
    SampleDocuments sampleDocuments = loadSampleDocuments(req, configSet);

    // Get a mutable "temp" schema either from the specified copy source or configSet if it already exists.
    String copyFrom = configExists(configSet) ? configSet
        : req.getParams().get(COPY_FROM_PARAM, DEFAULT_CONFIGSET_NAME);

    String mutableId = getMutableId(configSet);

    // holds additional settings needed by the designer to maintain state
    Map<String, Object> settings = new HashMap<>();
    ManagedIndexSchema schema = getMutableSchemaForConfigSet(configSet, schemaVersion, copyFrom, settings);

    String uniqueKeyFieldParam = req.getParams().get(UNIQUE_KEY_FIELD_PARAM);
    if (!isEmpty(uniqueKeyFieldParam)) {
      String uniqueKeyField = schema.getUniqueKeyField() != null ? schema.getUniqueKeyField().getName() : null;
      if (!uniqueKeyFieldParam.equals(uniqueKeyField)) {
        schema = updateUniqueKeyField(mutableId, schema, uniqueKeyFieldParam);
      }
    }

    boolean langsUpdated = false;
    String[] languages = req.getParams().getParams(LANGUAGES_PARAM);
    List<String> langs;
    if (languages != null) {
      langs = languages.length == 0 || (languages.length == 1 && "*".equals(languages[0])) ? Collections.emptyList() : Arrays.asList(languages);
      if (!langs.equals(settings.get(DESIGNER_KEY + LANGUAGES_PARAM))) {
        settings.put(DESIGNER_KEY + LANGUAGES_PARAM, langs);
        langsUpdated = true;
      }
    } else {
      // nothing from client, go with what's stored in the settings ...
      langs = (List<String>) settings.get(DESIGNER_KEY + LANGUAGES_PARAM);
    }

    boolean dynamicUpdated = false;
    Boolean enableDynamicFields = req.getParams().getBool(ENABLE_DYNAMIC_FIELDS_PARAM);
    if (enableDynamicFields != null && !enableDynamicFields.equals(getDesignerOption(settings, ENABLE_DYNAMIC_FIELDS_PARAM))) {
      settings.put(DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, enableDynamicFields);
      dynamicUpdated = true;
    }

    if (langsUpdated) {
      boolean dynamicEnabled = getDesignerOption(settings, ENABLE_DYNAMIC_FIELDS_PARAM);
      schema = configSetHelper.syncLanguageSpecificObjectsAndFiles(mutableId, schema, langs, dynamicEnabled, copyFrom);
    }

    if (dynamicUpdated) {
      if (!enableDynamicFields) {
        schema = configSetHelper.removeDynamicFields(schema);
      } else {
        schema = configSetHelper.restoreDynamicFields(schema, langs, copyFrom);
      }
    }

    List<SolrInputDocument> docs = sampleDocuments.parsed;
    if (!docs.isEmpty()) {
      if (ensureUniqueKey(schema.getUniqueKeyField(), docs)) {
        storeSampleDocs(configSet, docs);
      }
      schema = analyzeInputDocs(schemaSuggester.transposeDocs(docs), schema, langs);
    }

    // persist the updated schema
    if (!schema.persistManagedSchema(false)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to persist schema: " + mutableId);
    }

    Boolean enableFieldGuessing = req.getParams().getBool(ENABLE_FIELD_GUESSING_PARAM);
    if (enableFieldGuessing != null) {
      Boolean storedEnableFieldGuessing = getDesignerOption(settings, AUTO_CREATE_FIELDS);
      if (!enableFieldGuessing.equals(storedEnableFieldGuessing)) {
        settings.put(AUTO_CREATE_FIELDS, enableFieldGuessing);
      }
    }

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      configSetHelper.createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    } else {
      // already created in the prep step ... reload it to pull in the updated schema
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
    }

    // nested docs
    Boolean enableNestedDocs = req.getParams().getBool(ENABLE_NESTED_DOCS_PARAM);
    if (enableNestedDocs != null) {
      if (!enableNestedDocs.equals(getDesignerOption(settings, ENABLE_NESTED_DOCS_PARAM))) {
        settings.put(DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, enableNestedDocs);
        toggleNestedDocsFields(mutableId, schema, settings);
      }
    }

    // index the sample docs using the suggested schema
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (!docs.isEmpty()) {
      errorsDuringIndexing =
          indexSampleDocsWithRebuildOnAnalysisError(schema.getUniqueKeyField().getName(), docs, mutableId, false);
    }

    if (settingsDAO.persistIfChanged(mutableId, settings)) {
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
    }

    Map<String, Object> response = buildResponse(configSet, loadLatestSchema(mutableId, null), settings, docs);
    response.put("sampleSource", sampleDocuments.getSource());
    addErrorToResponse(mutableId, null, errorsDuringIndexing, response, null);
    rsp.getValues().addAll(response);
  }

  @EndPoint(method = GET, path = "/schema-designer/query", permission = CONFIG_READ_PERM)
  @SuppressWarnings("unchecked")
  public void query(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSet = getRequiredParam(CONFIG_SET_PARAM, req);
    String mutableId = getMutableId(configSet);
    if (!configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      configSetHelper.createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    }

    // only re-index if current state of test collection is not up-to-date
    int currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
    Integer version = indexedVersion.get(mutableId);
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (version == null || version != currentVersion) {
      log.debug("Schema for collection {} is stale ({} != {}), need to re-index sample docs", mutableId, version, currentVersion);
      List<SolrInputDocument> docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);
      ManagedIndexSchema schema = loadLatestSchema(mutableId, null);
      errorsDuringIndexing = indexSampleDocsWithRebuildOnAnalysisError(schema.getUniqueKeyField().getName(), docs, mutableId, true);
      // the version changes when you index (due to field guessing URP)
      currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
      indexedVersion.put(mutableId, currentVersion);
    }

    if (errorsDuringIndexing != null) {
      Map<String, Object> response = new HashMap<>();
      rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Failed to re-index sample documents after schema updated."));
      response.put(ERROR_DETAILS, errorsDuringIndexing);
      rsp.getValues().addAll(response);
      return;
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

  private byte[] streamAsBytes(final InputStream in) throws IOException {
    return DefaultSampleDocumentsLoader.streamAsBytes(in);
  }

  protected void storeSampleDocs(final String configSet, List<SolrInputDocument> docs) throws IOException {
    configSetHelper.postDataToBlobStore(cloudClient(), configSet + "_sample", streamAsBytes(toJavabin(docs)));
  }

  protected SampleDocuments loadSampleDocuments(SolrQueryRequest req, String configSet) throws IOException {
    List<SolrInputDocument> docs = null;
    ContentStream stream = extractSingleContentStream(req, false);
    SampleDocuments sampleDocs = null;
    if (stream != null && stream.getContentType() != null) {
      sampleDocs = sampleDocLoader.load(req.getParams(), stream, MAX_SAMPLE_DOCS);
      docs = sampleDocs.parsed;
      if (!docs.isEmpty()) {
        // user posted in some docs, if there are already docs stored in the blob store, then add these to the existing set
        List<SolrInputDocument> stored = configSetHelper.loadSampleDocsFromBlobStore(configSet);
        if (!stored.isEmpty()) {
          // keep the docs in the request as newest
          ManagedIndexSchema latestSchema = loadLatestSchema(getMutableId(configSet), null);
          docs = sampleDocs.appendDocs(latestSchema.getUniqueKeyField().getName(), stored, MAX_SAMPLE_DOCS);
        }

        // store in the blob store so we always have access to these docs
        storeSampleDocs(configSet, docs);
      }
    }

    if (docs == null || docs.isEmpty()) {
      // no sample docs in the request ... find in blob store (or fail if no docs previously stored)
      docs = configSetHelper.loadSampleDocsFromBlobStore(configSet);

      // no docs? but if this schema has already been published, it's OK, we can skip the docs part
      if (docs.isEmpty() && !configExists(configSet)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "No sample documents provided for analyzing schema! Only CSV/TSV, XML, JSON, and JSON lines supported.");
      }

      sampleDocs = new SampleDocuments(docs, "", "blob");
    }

    return sampleDocs;
  }

  protected boolean ensureUniqueKey(final SchemaField idField, List<SolrInputDocument> docs) {
    boolean updatedDocs = false;
    // if the unique key field is a string, we can supply a UUID if needed, otherwise must come from the user.
    if (StrField.class.equals(idField.getType().getClass())) {
      String idFieldName = idField.getName();
      for (SolrInputDocument d : docs) {
        if (d.getFieldValue(idFieldName) == null) {
          d.setField(idFieldName, UUID.randomUUID().toString().toLowerCase(Locale.ROOT));
          updatedDocs = true;
        }
      }
    }
    return updatedDocs;
  }

  protected ManagedIndexSchema analyzeInputDocs(final Map<String, List<Object>> docs, ManagedIndexSchema schema, List<String> langs) {
    // Adapt the provided schema to the sample docs
    for (String field : docs.keySet()) {
      List<Object> sampleValues = docs.getOrDefault(field, Collections.emptyList());

      // Collapse all whitespace in fields to a single underscore
      String normalizedField = field.trim().replaceAll("\\s+", "_");
      if (schema.hasExplicitField(normalizedField)) {
        SchemaField existing = schema.getField(normalizedField);
        schema = schemaSuggester.adaptExistingFieldToData(existing, sampleValues, schema);
        continue;
      }

      Optional<SchemaField> maybeSchemaField = schemaSuggester.suggestField(normalizedField, sampleValues, schema, langs);
      if (maybeSchemaField.isPresent()) {
        schema = (ManagedIndexSchema) schema.addField(maybeSchemaField.get(), false);
      }
    }

    return schema;
  }

  protected String getManagedSchemaZkPath(final String configSet) {
    return getConfigSetZkPath(configSet, DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME);
  }

  protected ManagedIndexSchema getMutableSchemaForConfigSet(final String configSet, final int schemaVersion, String copyFrom, Map<String, Object> settings) throws IOException, KeeperException, InterruptedException {
    // The designer works with mutable config sets stored in a "temp" znode in ZK instead of the "live" configSet
    final String mutableId = getMutableId(configSet);

    ManagedIndexSchema schema;

    // create new from the built-in "_default" configSet
    int publishedVersion = -1;
    boolean isNew = false;
    if (!configExists(mutableId)) {

      // are they opening a temp of an existing?
      if (configExists(configSet)) {
        if (settingsDAO.isDesignerDisabled(configSet)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Schema '" + configSet + "' is locked for edits by the schema designer!");
        }
        publishedVersion = configSetHelper.getCurrentSchemaVersion(configSet);
        // ignore the copyFrom as we're making a mutable temp copy of an already published configSet
        copyConfig(configSet, mutableId);
        copyFrom = configSet;
      } else {
        copyConfig(copyFrom, mutableId);
      }
      isNew = true;
    }

    SolrConfig solrConfig = configSetHelper.loadSolrConfig(mutableId);
    schema = configSetHelper.loadLatestSchema(solrConfig);
    if (!isNew) {
      // schema is not new, so the provided version must match, otherwise, we're trying to edit dirty data
      configSetHelper.checkSchemaVersion(mutableId, schemaVersion, schema.getSchemaZkVersion());
    }

    Map<String, Object> info = settingsDAO.getSettings(solrConfig);
    if (isNew) {
      if (!configSet.equals(copyFrom)) {
        info.put(DESIGNER_KEY + DISABLED, false);
      }

      // remember where this new one came from, unless the mutable is an edit of an already published schema,
      // in which case we want to preserve the original copyFrom
      info.putIfAbsent(DESIGNER_KEY + COPY_FROM_PARAM, copyFrom);

      if (publishedVersion != -1) {
        // keep track of the version of the configSet the mutable is derived from in case another user
        // changes the derived from schema before we publish the mutable on top of it
        info.put(DESIGNER_KEY + PUBLISHED_VERSION, publishedVersion);
      }

      if (!getDesignerOption(info, ENABLE_NESTED_DOCS_PARAM)) {
        schema = configSetHelper.deleteNestedDocsFieldsIfNeeded(schema, configSet, false);
      }

      if (!getDesignerOption(info, ENABLE_DYNAMIC_FIELDS_PARAM)) {
        schema = configSetHelper.removeDynamicFields(schema);
      }

      if (!schema.persistManagedSchema(false)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to persist temp schema: " + mutableId);
      }
    }

    settings.putAll(info); // optimization ~ return to the caller so we don't have to re-read the SolrConfig

    return schema;
  }

  ManagedIndexSchema loadLatestSchema(String configSet, Map<String, Object> settings) {
    SolrConfig solrConfig = configSetHelper.loadSolrConfig(configSet);
    if (settings != null) {
      settings.putAll(settingsDAO.getSettings(solrConfig));
    }
    return configSetHelper.loadLatestSchema(solrConfig);
  }

  protected ContentStream extractSingleContentStream(final SolrQueryRequest req, boolean required) {
    Iterable<ContentStream> streams = req.getContentStreams();
    Iterator<ContentStream> iter = streams != null ? streams.iterator() : null;
    ContentStream stream = iter != null && iter.hasNext() ? iter.next() : null;
    if (required && stream == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No JSON content found in the request body!");

    return stream;
  }

  protected CloudSolrClient cloudClient() {
    return coreContainer.getSolrClientCache().getCloudSolrClient(coreContainer.getZkController().getZkServerAddress());
  }

  protected ZkStateReader zkStateReader() {
    return coreContainer.getZkController().getZkStateReader();
  }

  protected Map<Object, Throwable> indexSampleDocsWithRebuildOnAnalysisError(String idField,
                                                                             List<SolrInputDocument> docs,
                                                                             final String collectionName,
                                                                             boolean asBatch) throws Exception {
    Map<Object, Throwable> results;
    try {
      results = indexSampleDocs(idField, docs, collectionName, asBatch);
    } catch (IllegalArgumentException analysisExc) {
      String errMsg = SolrException.getRootCause(analysisExc).getMessage();
      log.warn("Rebuilding temp collection {} after low-level Lucene indexing issue: {}", collectionName, errMsg);
      CollectionAdminRequest.deleteCollection(collectionName).process(cloudClient());
      zkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, Objects::isNull);
      configSetHelper.createCollection(collectionName, collectionName);
      results = indexSampleDocs(idField, docs, collectionName, asBatch);
      log.info("Re-index sample docs into {} after rebuild due to {} succeeded; results: {}", collectionName, errMsg, results);
    }
    return results;
  }

  protected Map<Object, Throwable> indexSampleDocs(String idField,
                                                   List<SolrInputDocument> docs,
                                                   final String collectionName,
                                                   boolean asBatch) throws Exception {

    Map<Object, Throwable> errorsDuringIndexing = new LinkedHashMap<>();

    RTimer timer = new RTimer();

    // load sample docs from blob store
    CloudSolrClient cloudSolrClient = cloudClient();
    cloudSolrClient.deleteByQuery(collectionName, "*:*", 1);
    cloudSolrClient.optimize(collectionName, true, true, 1);

    final int commitWithin = 100;
    final int numDocs = docs.size();
    int numAdded = 0;
    if (asBatch) {
      cloudSolrClient.add(collectionName, docs, commitWithin);
      numAdded = docs.size();
    } else {
      int maxErrors = Math.min(100, Math.round(numDocs / 2f));
      for (SolrInputDocument next : docs) {
        try {
          cloudSolrClient.add(collectionName, next, commitWithin);
          ++numAdded;
        } catch (Exception exc) {
          Throwable rootCause = SolrException.getRootCause(exc);
          if (String.valueOf(rootCause.getMessage()).contains("possible analysis error")) {
            throw new IllegalArgumentException(rootCause);
          } else {
            Object docId = next.getFieldValue(idField);
            if (docId == null) {
              throw exc;
            }
            errorsDuringIndexing.put(docId, rootCause);

            // break if there are a lot of errors in indexing as something is very wrong if so ...
            if (errorsDuringIndexing.size() > 20 && errorsDuringIndexing.size() >= maxErrors) {
              break;
            }
          }
        }
      }
    }

    cloudSolrClient.commit(collectionName, true, true, true);

    if (!errorsDuringIndexing.isEmpty()) {
      return errorsDuringIndexing;
    }

    long numFound = waitToSeeSampleDocs(collectionName, numAdded);
    double tookMs = timer.getTime();
    log.debug("Indexed {} docs into collection {}, took {} ms", numFound, collectionName, tookMs);

    return !errorsDuringIndexing.isEmpty() ? errorsDuringIndexing : null;
  }

  protected long waitToSeeSampleDocs(String collectionName, long numAdded) throws IOException, SolrServerException {
    CloudSolrClient cloudSolrClient = cloudClient();
    SolrQuery query = new SolrQuery("*:*");
    query.setRows(0);
    QueryResponse queryResponse = cloudSolrClient.query(collectionName, query);
    long numFound = queryResponse.getResults().getNumFound();
    if (numFound < numAdded) {
      // wait up to 5 seconds for this to occur
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      do {
        cloudSolrClient.commit(collectionName, true, true, true);
        queryResponse = cloudSolrClient.query(collectionName, query);
        numFound = queryResponse.getResults().getNumFound();
        if (numFound >= numAdded) {
          break;
        }
        try {
          Thread.sleep(200); // little pause to avoid flooding the server with requests in this loop
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      } while (System.nanoTime() < deadline);

      if (numFound < numAdded) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Failed to index " + numAdded + " sample docs into temp collection: " + collectionName);
      }
    }
    return numFound;
  }

  protected Map<String, Object> buildResponse(String configSet,
                                              final ManagedIndexSchema schema,
                                              Map<String, Object> settings,
                                              List<SolrInputDocument> docs) throws Exception {
    String mutableId = getMutableId(configSet);
    int currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
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
    response.put("collectionsForConfig", configSetHelper.listCollectionsForConfig(configSet));
    // Guess at a schema for each field found in the sample docs
    // Collect all fields across all docs with mapping to values
    response.put("fields", schema.getFields().values().stream()
        .map(f -> fieldToMap(f, schema))
        .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
        .collect(Collectors.toList()));

    if (settings == null) {
      settings = settingsDAO.getSettings(configSetHelper.loadSolrConfig(mutableId));
    }
    addSettingsToResponse(settings, response);

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
    String configPathInZk = getConfigSetZkPath(mutableId);
    final Set<String> files = new HashSet<>();
    ZkMaintenanceUtils.traverseZkTree(zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, files::add);
    files.remove(configPathInZk);

    final String prefix = configPathInZk + "/";
    final int prefixLen = prefix.length();
    Set<String> stripPrefix = files.stream().map(f -> f.startsWith(prefix) ? f.substring(prefixLen) : f).collect(Collectors.toSet());
    stripPrefix.remove(DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME);
    stripPrefix.remove("lang");
    stripPrefix.remove(CONFIGOVERLAY_JSON); // treat this file as private

    List<String> sortedFiles = new ArrayList<>(stripPrefix);
    Collections.sort(sortedFiles);
    response.put("files", sortedFiles);

    // info about the sample docs
    if (docs != null) {
      final String uniqueKeyField = schema.getUniqueKeyField().getName();
      response.put("docIds", docs.stream()
          .map(d -> (String) d.getFieldValue(uniqueKeyField))
          .filter(Objects::nonNull)
          .limit(100)
          .collect(Collectors.toList()));
    }

    response.put("numDocs", docs != null ? docs.size() : -1);

    return response;
  }

  protected void addErrorToResponse(String collection,
                                    SolrException solrExc,
                                    Map<Object, Throwable> errorsDuringIndexing,
                                    Map<String, Object> response,
                                    String updateError) {

    if (solrExc == null && (errorsDuringIndexing == null || errorsDuringIndexing.isEmpty())) {
      return; // no errors
    }

    if (updateError != null) {
      response.put(UPDATE_ERROR, updateError);
    }

    if (solrExc != null) {
      response.put("updateErrorCode", solrExc.code());
      response.putIfAbsent(UPDATE_ERROR, solrExc.getMessage());
    }

    response.putIfAbsent(UPDATE_ERROR, "Index sample documents into " + collection + " failed!");
    response.putIfAbsent("updateErrorCode", 400);
    if (errorsDuringIndexing != null) {
      response.put(ERROR_DETAILS, errorsDuringIndexing);
    } else if (solrExc != null) {
      StringWriter sw = new StringWriter();
      solrExc.printStackTrace(new PrintWriter(sw));
      response.put(ERROR_DETAILS, sw.toString());
    }
  }

  protected SimpleOrderedMap<Object> fieldToMap(SchemaField f, ManagedIndexSchema schema) {
    SimpleOrderedMap<Object> map = f.getNamedPropertyValues(true);

    // add the copy field destination field names
    List<String> copyFieldNames =
        schema.getCopyFieldsList((String) map.get("name")).stream().map(c -> c.getDestination().getName()).collect(Collectors.toList());
    map.add("copyDest", String.join(",", copyFieldNames));

    return map;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> readJsonFromRequest(SolrQueryRequest req) throws IOException {
    ContentStream stream = extractSingleContentStream(req, true);
    String contentType = stream.getContentType();
    if (isEmpty(contentType) || !contentType.toLowerCase(Locale.ROOT).contains(JSON_MIME)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected JSON in request!");
    }
    final Object json;
    try (Reader reader = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(reader));
    }
    return (Map<String, Object>) json;
  }

  protected void addSettingsToResponse(Map<String, Object> settings, final Map<String, Object> response) {
    settings.forEach((key, value) -> {
      if (value != null) {
        if (key.startsWith(DESIGNER_KEY)) {
          key = key.substring(DESIGNER_KEY.length());
        } else if (AUTO_CREATE_FIELDS.equals(key)) {
          key = ENABLE_FIELD_GUESSING_PARAM;
        }
        response.put(key, value);
      }
    });
  }

  protected String checkMutable(String configSet, SolrQueryRequest req) throws IOException, KeeperException, InterruptedException {
    // an apply just copies over the temp config to the "live" location
    String mutableId = getMutableId(configSet);
    if (!configExists(mutableId)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          mutableId + " configSet not found! Are you sure " + configSet + " was being edited by the schema designer?");
    }

    // check the versions agree
    configSetHelper.checkSchemaVersion(mutableId, requireSchemaVersionFromClient(req), -1);

    return mutableId;
  }

  protected int requireSchemaVersionFromClient(SolrQueryRequest req) {
    final int schemaVersion = req.getParams().getInt(SCHEMA_VERSION_PARAM, -1);
    if (schemaVersion == -1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          SCHEMA_VERSION_PARAM + " is a required parameter for the " + req.getPath() + " endpoint");
    }
    return schemaVersion;
  }

  protected String getRequiredParam(final String param, final SolrQueryRequest req) {
    final String paramValue = req.getParams().get(param);
    if (isEmpty(paramValue)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          param + " is a required parameter for the " + req.getPath() + " endpoint!");
    }
    return paramValue;
  }

  protected ManagedIndexSchema updateUniqueKeyField(String mutableId, ManagedIndexSchema schema, String uniqueKeyField) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Changing the unique key field not supported yet!");
  }

  protected void toggleNestedDocsFields(String mutableId, ManagedIndexSchema schema, Map<String, Object> settings) throws IOException, SolrServerException {
    configSetHelper.toggleNestedDocsFields(mutableId, schema, getDesignerOption(settings, ENABLE_NESTED_DOCS_PARAM));
  }

  protected boolean getDesignerOption(Map<String, Object> settings, String propName) {
    Boolean option = (Boolean) settings.get(DESIGNER_KEY + propName);
    if (option == null) {
      option = (Boolean) settings.get(propName);
    }
    if (option == null) {
      throw new IllegalStateException(propName + " not found in designer settings: " + settings);
    }
    return option;
  }

  protected void cleanupTemp(String configSet) throws IOException, SolrServerException {
    String mutableId = getMutableId(configSet);
    indexedVersion.remove(mutableId);
    CloudSolrClient cloudSolrClient = cloudClient();
    CollectionAdminRequest.deleteCollection(mutableId).process(cloudSolrClient);
    // delete the sample doc blob
    cloudSolrClient.deleteByQuery(".system", "id:" + configSet + "_sample/*", 10);
    cloudSolrClient.commit(".system", true, true);
    deleteConfig(mutableId);
  }

  private boolean configExists(String configSet) throws IOException {
    return coreContainer.getConfigSetService().checkConfigExists(configSet);
  }

  private void deleteConfig(String configSet) throws IOException {
    coreContainer.getConfigSetService().deleteConfig(configSet);
  }

  private void copyConfig(String from, String to) throws IOException {
    coreContainer.getConfigSetService().copyConfig(from, to);
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
