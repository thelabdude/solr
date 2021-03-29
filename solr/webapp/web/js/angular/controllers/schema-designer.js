/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

solrAdminApp.controller('SchemaDesignerController', function ($scope, $timeout, $cookies, $window, Constants, SchemaDesigner, Luke) {
  $scope.resetMenu("schema-designer", Constants.IS_ROOT_PAGE);

  $scope.onError = function(errorMsg, errorCode) {
    $scope.updateWorking = false;
    delete $scope.updateStatusMessage;
    var detailsAt = errorMsg.indexOf(" Details: ");
    if (detailsAt !== -1) {
      $scope.designerAPIError = errorMsg.substring(0, detailsAt);
      $scope.designerAPIErrorDetails = errorMsg.substring(detailsAt+10);
    } else {
      $scope.designerAPIError = errorMsg;
      delete $scope.designerAPIErrorDetails;
    }
    if (errorCode === 409) {
      $scope.schemaVersion = -1; // reset to get the latest
      $scope.isVersionMismatch = true;
      $scope.errorButton = "Reload Schema";
    } else if (errorCode < 500) {
      $scope.isVersionMismatch = false;
      $scope.errorButton = "OK";
    } // else 500 errors get the top-level error message
  };
  
  $scope.errorHandler = function(e) {
    var error = e.data ? e.data.error : null;
    if (error) {
      $scope.onError(error.msg, error.code);
    }
  };

  $scope.closeErrorDialog = function() {
    delete $scope.designerAPIError;
    delete $scope.designerAPIErrorDetails;
    if ($scope.isVersionMismatch) {
      $scope.isVersionMismatch = false;
      var nodeId = "/";
      if ($scope.selectedNode) {
        nodeId = $scope.selectedNode.href;
      }
      $scope.doAnalyze(nodeId);
    }
  };

  $scope.refresh = function () {
    $scope.updateStatusMessage = "";
    $scope.analysisVerbose = false;
    $scope.updateWorking = false;

    delete $scope.hasDocsOnServer;
    delete $scope.queryResultsTree;

    $scope.languages = ["*"];
    $scope.copyFrom = "_default";
    delete $scope.sampleMessage;
    delete $scope.sampleDocuments;
    delete $scope.fileUpload;

    $scope.schemaVersion = -1;
    $scope.schemaTree = {};
    $scope.showSchemaActions = false;
    $scope.sampleDocIds = [];
    $scope.isSchemaRoot = false;

    delete $scope.enableNestedDocs;
    delete $scope.enableDynamicFields;
    delete $scope.enableFieldGuessing;

    // schema editor
    $scope.showFieldDetails = false;
    $scope.selectedNode = null;
    $scope.selectedUpdated = false;
    delete $scope.updateStatusMessage;

    // text field analysis
    $scope.showAnalysis = false;
    $scope.sampleDocId = null;
    $scope.indexText = "";
    $scope.result = {};

    // publish vars
    delete $scope.newCollection;
    $scope.reloadOnPublish = "true";

    // query form
    $scope.query = {q: '*:*', sortBy:'score', sortDir:'desc'};

    SchemaDesigner.get({path: "configs"}, function (data) {
      $scope.schemas = data.configSets

      // slightly hacky but meh
      $scope.schemasWithDefault = [...data.configSets];
      $scope.schemasWithDefault.push("_default");

      // if no schemas available to select, open the pop-up immediately
      if ($scope.schemas.length == 0) {
        $scope.firstSchemaMessage = true;
        $scope.showNewSchemaDialog();
      }
    });
  };

  $scope.loadFile = function(event) {
    var t = event.target || event.srcElement || event.currentTarget;
    if (t && t.text) {
      $scope.onSelectFileNode("files/"+t.text, true);
    }
  };

  $scope.confirmEditSchema = function() {
    $scope.showConfirmEditSchema = false;
    if ($scope.hasDocsOnServer || $scope.published) {
      $scope.doAnalyze();
    } else {
      $scope.sampleMessage = "Please upload or paste some sample documents to analyze for building the '" + $scope.currentSchema + "' schema.";
    }
  };

  $scope.cancelEditSchema = function() {
    delete $scope.currentSchema;
    $scope.showConfirmEditSchema = false;
  };

  $scope.loadSchema = function (schema) {
    if (!schema) {
      return;
    }

    var params = {path: "info"};
    params.configSet = schema;

    SchemaDesigner.get(params, function (data) {
      $scope.confirmSchema = schema;
      $scope.collectionsForConfig = data.collections;
      $scope.hasDocsOnServer = data.numDocs > 0;
      $scope.published = data.published;
      $scope.initDesignerSettingsFromResponse(data);
      if ($scope.collectionsForConfig && $scope.collectionsForConfig.length > 0) {
        $scope.showConfirmEditSchema = true;
      } else {
        if ($scope.hasDocsOnServer|| $scope.published) {
          $scope.doAnalyze();
        } else {
          $scope.sampleMessage = "Please upload or paste some sample documents to build the '" + $scope.currentSchema + "' schema.";
        }
      }
    });
  };

  $scope.showNewSchemaDialog = function () {
    $scope.hideAll();
    $scope.showNewSchema = true;
    $scope.newSchema = "";
  };

  $scope.addSchema = function () {
    delete $scope.sampleMessage;
    $scope.firstSchemaMessage = false;

    if (!$scope.newSchema) {
      $scope.addMessage = "Please provide a schema name!";
      return;
    }

    if ($scope.newSchema.indexOf(" ") !== -1 || $scope.newSchema.indexOf("/") !== -1) {
      $scope.addMessage = "Schema name should not contain spaces or /";
      return;
    }

    if ($scope.schemasWithDefault.includes($scope.newSchema)) {
      $scope.addMessage = "Schema '" + $scope.newSchema + "' already exists!";
      return;
    }

    if (!$scope.copyFrom) {
      $scope.copyFrom = "_default";
    }

    $scope.resetSchema();
    $scope.schemas.push($scope.newSchema);
    $scope.schemasWithDefault.push($scope.newSchema);
    $scope.showNewSchema = false;
    $scope.currentSchema = $scope.newSchema;
    $scope.sampleMessage = "Please upload or paste some sample documents to analyze for building the '" + $scope.currentSchema + "' schema.";

    SchemaDesigner.post({path: "prep", configSet: $scope.newSchema, copyFrom: $scope.copyFrom}, null, function (data) {
      // no-op by design ... we want this to run in the bg
    }, $scope.errorHandler);
  };

  $scope.cancelAddSchema = function () {
    delete $scope.addMessage;
    delete $scope.sampleMessage;

    $scope.showNewSchema = false
  };

  $scope.hideAll = function () {
    delete $scope.helpId;
    $scope.showPublish = false;
    $scope.showNewSchema = false;
    $scope.showAddField = false;
    $scope.showAddDynamicField = false;
    $scope.showAddCopyField = false;
    $scope.showAnalysis = false;
    // add more dialogs here
  };

  $scope.showHelp = function(id) {
    if ($scope.helpId && ($scope.helpId === id || id === '')) {
      delete $scope.helpId;
    } else {
      $scope.helpId = id;
    }
  };

  $scope.hideData = function () {
    $scope.showData = false;
  };

  $scope.rootChanged = function() {
    $scope.selectedUpdated = true;
    $scope.selectedType = "Schema";
  };

  $scope.updateUniqueKey = function () {
    delete $scope.schemaRootMessage;
    var jst = $('#schemaJsTree').jstree();
    if (jst) {
      var node = jst.get_node("field/" + $scope.updateUniqueKeyField);
      if (node && node.a_attr) {
        var attrs = node.a_attr;
        if (attrs.multiValued || attrs.tokenized || !attrs.stored || !attrs.indexed) {
          $scope.schemaRootMessage = "Field '" + $scope.updateUniqueKeyField +
              "' cannot be used as the uniqueKey field! Must be single-valued, stored, indexed, and not tokenized.";
          $scope.updateUniqueKeyField = $scope.uniqueKeyField;
          return;
        }
      }
    }
    $scope.uniqueKeyField = $scope.updateUniqueKeyField;
    $scope.selectedUpdated = true;
    $scope.selectedType = "Schema";
  };

  $scope.resetSchema = function() {
    $scope.hideAll();
    $scope.analysisVerbose = false;
    $scope.showSchemaActions = false;
    $scope.showAnalysis = false;
    $scope.showFieldDetails = false;
    $scope.hasDocsOnServer = false;
    $scope.query = {q: '*:*', sortBy:'score', sortDir:'desc'};
    $scope.sampleDocuments = "";
    $scope.schemaVersion = -1;

    $scope.updateWorking = false;
    $scope.isVersionMismatch = false;
    delete $scope.updateStatusMessage;
    delete $scope.designerAPIError;
    delete $scope.designerAPIErrorDetails;
    delete $scope.selectedFacets;
    delete $scope.fileUpload;
    delete $scope.sampleDocuments;
    delete $scope.selectedNode;
    delete $scope.queryResultsTree;
  };

  $scope.onSchemaUpdated = function (schema, data, nodeId) {
    delete $scope.fileUpload;

    $scope.hasDocsOnServer = data.numDocs && data.numDocs > 0;
    $scope.uniqueKeyField = data.uniqueKeyField;
    $scope.updateUniqueKeyField = $scope.uniqueKeyField;
    $scope.initDesignerSettingsFromResponse(data);

    var fieldTypes = fieldTypesToTree(data.fieldTypes);
    var files = filesToTree(data.files);

    var rootChildren = [];
    rootChildren.push({"text": "Fields", "state": {"opened": true}, "a_attr": {"href": "fields"}, "children": fieldsToTree(data.fields)});

    if ($scope.enableDynamicFields === "true") {
      var dynamicFields = fieldsToTree(data.dynamicFields);
      rootChildren.push({"text": "Dynamic Fields", "a_attr": {"href": "dynamicFields"}, "children": dynamicFields});
    }

    rootChildren.push({"text": "Field Types", "a_attr": {"href": "fieldTypes"}, "children": fieldTypes});
    rootChildren.push(files);

    var tree = [{"text": schema, "a_attr": {"href": "/"}, "children": rootChildren}];

    $scope.fields = data.fields;
    $scope.fieldNames = data.fields.map(f => f.name).sort();
    $scope.possibleIdFields = data.fields.filter(f => f.indexed && f.stored && !f.tokenized).map(f => f.name).sort();
    $scope.sortableFields = data.fields.filter(f => (f.indexed || f.docValues) && !f.multiValued).map(f => f.name).sort();
    $scope.sortableFields.push("score");
    $scope.facetFields = data.fields.filter(f => (f.indexed || f.docValues) && !f.tokenized && f.name !== '_version_').map(f => f.name).sort();

    $scope.schemaVersion = data.schemaVersion;
    $scope.currentSchema = data.configSet;
    $scope.fieldTypes = fieldTypes;
    $scope.core = data.core;
    $scope.schemaTree = tree;
    $scope.collectionsForConfig = data.collectionsForConfig;

    if (data.docIds) {
      $scope.sampleDocIds = data.docIds;
    }

    // Load the Luke schema
    Luke.schema({core: data.core}, function (schema) {
      Luke.raw({core: data.core}, function (index) {
        $scope.luke = mergeIndexAndSchemaData(index, schema.schema);
        $scope.types = Object.keys(schema.schema.types);
        $scope.showSchemaActions = true;
        if (!nodeId) {
          nodeId = "/";
        }
        $scope.onSelectSchemaTreeNode(nodeId);
        $scope.updateWorking = false;
        if (data.updateError != null) {
          $scope.onError(data.updateError, data.updateErrorCode);
        }  else {
          if ($scope.selectedUpdated) {
            $scope.selectedUpdated = false;
            $scope.updateStatusMessage = "Changes applied successfully.";
            $timeout(function () {
              delete $scope.updateStatusMessage;
            }, 2000);
          } else {
            delete $scope.updateStatusMessage;
          }
        }
        
        // re-fire the current query to reflect the updated schema
        $scope.doQuery();
      });
    });
  };

  $scope.toggleAddField = function (type) {
    if ($scope.showAddField) {
      $scope.hideAll();
    } else {
      $scope.hideAll();
      $scope.showAddField = true;
      $scope.adding = type;

      $scope.newField = {
        stored: "true",
        indexed: "true",
        uninvertible: "true"
      }
      delete $scope.addErrors;
    }
  };

  function applyConstraintsOnField(f) {

    if (!f.docValues) {
      delete f.useDocValuesAsStored;
    }

    if (!f.docValues && !f.uninvertible) {
      delete f.sortMissingLast; // remove this setting if no docValues / uninvertible
    }

    if (f.indexed) {
      if (f.omitTermFreqAndPositions && !f.omitPositions) {
        delete f.omitPositions; // :shrug ~ see SchemaField ln 295
      }
      if (!f.termVectors) {
        delete f.termPositions;
        delete f.termOffsets;
        delete f.termPayloads;
      }
    } else {
      // if not indexed, a bunch of fields are false
      f.tokenized = false;
      f.uninvertible = false;

      // drop these from the request
      delete f.termVectors;
      delete f.termPositions;
      delete f.termOffsets;
      delete f.termPayloads;
      delete f.omitNorms;
      delete f.omitPositions;
      delete f.omitTermFreqAndPositions;
      delete f.storeOffsetsWithPositions;
    }

    return f;
  }

  $scope.addField = function () {

    // validate the form input
    $scope.addErrors = [];
    if (!$scope.newField.name) {
      $scope.addErrors.push($scope.adding + " name is required!");
    }

    if ($scope.newField.name.indexOf(" ") != -1) {
      $scope.addErrors.push($scope.adding + " name should not have whitespace");
    }

    var command = "add-field-type";
    if ("field" === $scope.adding) {
      // TODO: is this the correct logic for detecting dynamic? Probably good enough for the designer
      var isDynamic = $scope.newField.name.startsWith("*") || $scope.newField.name.endsWith("*");
      if (isDynamic) {
        if ($scope.luke && $scope.luke.dynamic_fields[$scope.newField.name]) {
          $scope.addErrors.push("dynamic field '" + $scope.newField.name + "' already exists!");
        }
      } else {
        if ($scope.luke && $scope.luke.fields[$scope.newField.name]) {
          $scope.addErrors.push("field '" + $scope.newField.name + "' already exists!");
        }
      }

      if (!$scope.newField.type) {
        $scope.addErrors.push("field type is required!");
      }

      command = isDynamic ? "add-dynamic-field" : "add-field";
    }

    if ($scope.addErrors.length > 0) {
      return;
    }
    delete $scope.addErrors; // no errors!

    var addData = {};
    addData[command] = applyConstraintsOnField($scope.newField);

    SchemaDesigner.post({path: "add", configSet: $scope.currentSchema, schemaVersion: $scope.schemaVersion}, addData, function (data) {
      if (data.errors) {
        $scope.addErrors = data.errors[0].errorMessages;
        if (typeof $scope.addErrors === "string") {
          $scope.addErrors = [$scope.addErrors];
        }
      } else {
        $scope.added = true;
        $timeout(function () {
          $scope.showAddField = false;
          $scope.added = false;
          var nodeId = "/";
          if ("field" === $scope.adding) {
            nodeId = "field/"+data[command];
          }
          $scope.onSchemaUpdated(data.configSet, data, nodeId);
        }, 500);
      }
    }, $scope.errorHandler);
  }

  $scope.togglePublish = function (event) {
    if (event) {
      var t = event.target || event.currentTarget;
      var leftPos = t.getBoundingClientRect().left - 500;
      if (leftPos < 0) leftPos = 0;
      $('#publish-dialog').css({left: leftPos});
    }

    $scope.showPublish = !$scope.showPublish;
    delete $scope.publishErrors;
    if ($scope.showPublish && !$scope.newCollection) {
      $scope.newCollection = {numShards: 1, replicationFactor: 1, indexToCollection: "true"};
    }
  };

  $scope.toggleAddCopyField = function () {
    if ($scope.showAddCopyField) {
      $scope.hideAll();
      $scope.showFieldDetails = true;
    } else {
      $scope.hideAll();
      $scope.showAddCopyField = true;
      $scope.showFieldDetails = false;

      $scope.copyField = {};
      delete $scope.addCopyFieldErrors;
    }
  }
  $scope.addCopyField = function () {
    delete $scope.addCopyFieldErrors;
    var data = {"add-copy-field": $scope.copyField};
    SchemaDesigner.post({path: "add", configSet: $scope.currentSchema, schemaVersion: $scope.schemaVersion}, data, function (data) {
      if (data.errors) {
        $scope.addCopyFieldErrors = data.errors[0].errorMessages;
        if (typeof $scope.addCopyFieldErrors === "string") {
          $scope.addCopyFieldErrors = [$scope.addCopyFieldErrors];
        }
      } else {
        $scope.showAddCopyField = false;
        // TODO:
        //$timeout($scope.refresh, 1500);
      }
    }, $scope.errorHandler);
  }

  $scope.toggleAnalyzer = function (analyzer) {
    analyzer.show = !analyzer.show;
  }

  $scope.initTypeAnalysisInfo = function (typeName) {
    $scope.analysis = getAnalysisInfo($scope.luke, { type: true }, typeName);
    if ($scope.analysis && $scope.analysis.data) {
      $scope.className = $scope.analysis.data.className
    }
  };

  $scope.toggleVerbose = function() {
    $scope.analysisVerbose = !$scope.analysisVerbose;
  };

  $scope.updateSampleDocId = function () {
    $scope.indexText = "";
    $scope.result = {};

    if (!$scope.selectedNode) {
      return;
    }

    var field = $scope.selectedNode.name;
    var params = {path: "sample"};
    params.configSet = $scope.currentSchema;
    params.uniqueKeyField = $scope.uniqueKeyField;
    params.field = field;

    if ($scope.sampleDocId) {
      params.docId = $scope.sampleDocId;
    } // else the server will pick the first doc with a non-empty text value for the desired field

    SchemaDesigner.get(params, function (data) {
      $scope.sampleDocId = data[$scope.uniqueKeyField];
      $scope.indexText = data[field];
      if (data.analysis && data.analysis["field_names"]) {
        $scope.result = processFieldAnalysisData(data.analysis["field_names"][field]);
      }
    });
  };

  $scope.changeLanguages = function () {
    $scope.selectedUpdated = true;
    $scope.selectedType = "Schema";
  };

  function getType(typeName) {
    if ($scope.fieldTypes) {
      for (i in $scope.fieldTypes) {
        if ($scope.fieldTypes[i].text === typeName) {
          return $scope.fieldTypes[i];
        }
      }
    }
    return null;
  }

  $scope.onSchemaTreeLoaded = function (id) {
    //console.log(">> on tree loaded");
  };

  $scope.updateFile = function() {
    var nodeId = "files/"+$scope.selectedFile;
    var params = {path: "file", file: $scope.selectedFile, configSet: $scope.currentSchema};

    SchemaDesigner.post(params, $scope.fileNodeText, function (data) {
      if (data.updateFileError) {
        if (data[$scope.selectedFile]) {
          $scope.fileNodeText = data[$scope.selectedFile];
        }
        $scope.updateFileError = data.updateFileError;
      } else {
        delete $scope.updateFileError;
        $scope.onSchemaUpdated(data.configSet, data, nodeId);
      }
    }, $scope.errorHandler);
  };

  $scope.onSelectFileNode = function(id, doSelectOnTree) {
    $scope.selectedFile = id.startsWith("files/") ? id.substring("files/".length) : id;
    var params = {path: "file", file: $scope.selectedFile, configSet: $scope.currentSchema};
    SchemaDesigner.get(params, function (data) {
      $scope.fileNodeText = data[$scope.selectedFile];
      $scope.isLeafNode = false;
      if (doSelectOnTree) {
        var jst = $('#schemaJsTree').jstree();
        var node = jst.get_node(id);
        if (node) {
          var selected_node = jst.get_selected();
          if (selected_node) {
            jst.deselect_node(selected_node);
          }
          delete $scope.selectedNode;
          $scope.isLeafNode = false;
          $scope.showFieldDetails = true;
          delete $scope.sampleDocId;
          $scope.showAnalysis = false;
          jst.select_node(id);
        }
      }
    });
  };

  $scope.onSelectSchemaTreeNode = function (id) {
    $scope.showFieldDetails = false;

    delete $scope.selectedFile;

    if (id === "/") {
      $scope.selectedType = "Schema";
      $scope.selectedNode = null;
      $scope.isSchemaRoot = true;
      $scope.isLeafNode = false;
      $scope.showFieldDetails = true;
      delete $scope.sampleDocId;
      $scope.showAnalysis = false;
      return;
    }

    $scope.isSchemaRoot = false;
    $scope.isLeafNode = false;

    if (id.indexOf("/") == -1) {
      $scope.selectedNode = null;
      $scope.isLeafNode = false;
      $scope.showFieldDetails = true;
      delete $scope.sampleDocId;
      $scope.showAnalysis = false;
      return;
    }

    var jst = $('#schemaJsTree').jstree();
    var node = jst.get_node(id);
    if (!node) {
      return;
    }

    if (id.startsWith("files/")) {
      $scope.selectedNode = null;
      $scope.isLeafNode = false;
      $scope.showFieldDetails = true;
      delete $scope.sampleDocId;
      $scope.showAnalysis = false;
      $scope.onSelectFileNode(id, false);
      return;
    }

    delete $scope.selectedFile;

    $scope.selectedNode = node["a_attr"]; // all the info we need is in the a_attr object
    if (!$scope.selectedNode) {
      // a node in the tree that isn't a field was selected, just ignore
      return;
    }
    $scope.selectedNode.fieldType = getType($scope.selectedNode.type);
    $scope.isLeafNode = true;

    var nodeType = id.substring(0, id.indexOf("/"));
    var name = null;
    if (nodeType == "field") {
      $scope.selectedType = "Field";
      name = $scope.selectedNode.type;
    } else if (nodeType == "dynamic") {
      $scope.selectedType = "Dynamic Field";
    } else if (nodeType == "type") {
      $scope.selectedType = "Type";
      name = $scope.selectedNode.name;
    }

    if (name) {
      $scope.initTypeAnalysisInfo(name, "type");
    }

    // apply some sanity to the checkboxes
    $scope.selectedNode = applyConstraintsOnField($scope.selectedNode);
    $scope.showFieldDetails = true;

    if ($scope.selectedNode.tokenized) {
      $scope.showAnalysis = true;
      $scope.updateSampleDocId();
    } else {
      $scope.showAnalysis = false;
      $scope.indexText = "";
      $scope.result = {};
    }
  };

  function addFileNode(dirs, parent, f) {
    var path = f.split("/");
    if (path.length === 1) {
      if (!parent.children) {
        parent.children = [];
        dirs.push(parent); // now parent has children, so track in dirs ...
      }
      var nodeId = parent.id + "/" + f;
      parent.children.push({"text": f, "id": nodeId, "a_attr": {"href": nodeId}});
    } else {
      // get the parent for this path
      var parentId = "files/" + path.slice(0, path.length - 1).join("/");
      var dir = null;
      for (var d in dirs) {
        if (dirs[d].id === parentId) {
          dir = dirs[d];
          break;
        }
      }
      if (!dir) {
        dir = {"text": path[0], "id": parentId, "a_attr": {"href": parentId}, "children": []};
        dirs.push(dir);
        parent.children.push(dir);
      }

      // walk down the next level in this path
      addFileNode(dirs, dir, path.slice(1).join("/"));
    }
  }

  // transform a flat list structure into the nested tree structure
  function filesToTree(files) {
    var filesNode = {"text": "Files", "a_attr": {"href": "files"}, "id": "files", "children": []};
    if (files) {
      var dirs = []; // lookup for known dirs by path since nodes don't keep a ref to their parent node
      for (var i in files) {
        // hide the configoverlay.json from the UI
        if (files[i] === "configoverlay.json") {
          continue;
        }

        addFileNode(dirs, filesNode, files[i]);
      }
      delete dirs;
    }
    return filesNode;
  }

  function fieldsToTree(fields) {
    var children = [];
    if (fields) {
      for (var i in fields) {
        var id = "field/" + fields[i].name;
        fields[i].href = id;
        var text = fields[i].name;
        if (fields[i].name === $scope.uniqueKeyField) {
          text += "*"; // unique key field
        }
        children.push({"text": text, "a_attr": fields[i], "id": id});
      }
    }
    return children;
  }

  function fieldTypesToTree(types) {
    var children = [];
    for (var i in types) {
      var ft = types[i]
      var id = "type/" + ft.name;
      ft.href = id;
      children.push({"text": ft.name, "a_attr": ft, "id": id});
    }
    return children;
  }

  $scope.onSampleDocumentsChanged = function() {
    $scope.hasDocsOnServer = false; // so the updates get sent on next analyze action
  };

  $scope.initDesignerSettingsFromResponse = function(data) {
    $scope.enableDynamicFields = data.enableDynamicFields !== null ? ""+data.enableDynamicFields : "true";
    $scope.enableFieldGuessing = data.enableFieldGuessing !== null ? ""+data.enableFieldGuessing : "true";
    $scope.enableNestedDocs = data.enableNestedDocs !== null ? ""+data.enableNestedDocs : "false";
    $scope.languages = data.languages !== null && data.languages.length > 0 ? data.languages : ["*"];
    $scope.copyFrom = data.copyFrom !== null ? data.copyFrom : "_default";
  };

  $scope.doAnalyze = function (nodeId) {
    delete $scope.sampleMessage;

    var schema = $scope.currentSchema;
    if (schema) {
      delete $scope.copyFrom;
    } else {
      schema = $scope.newSchema;
      if (!$scope.copyFrom) {
        $scope.copyFrom = "_default";
      }
    }

    if (!schema) {
      return;
    }

    var params = {path: "analyze", configSet: schema};
    if ($scope.schemaVersion && $scope.schemaVersion !== -1) {
      params.schemaVersion = $scope.schemaVersion;
    }

    if ($scope.enableDynamicFields) {
      params.enableDynamicFields = $scope.enableDynamicFields;
    }
    if ($scope.enableFieldGuessing) {
      params.enableFieldGuessing = $scope.enableFieldGuessing;
    }
    if ($scope.enableNestedDocs) {
      params.enableNestedDocs = $scope.enableNestedDocs;
    }

    if ($scope.languages && $scope.languages.length > 0) {
      params.languages = $scope.languages;
    }

    if ($scope.copyFrom) {
      params.copyFrom = $scope.copyFrom;
    }

    $scope.updateWorking = true;
    if ($scope.selectedUpdated) {
      $scope.updateStatusMessage = "Applying "+$scope.selectedType+" updates ..."
    } else {
      $scope.updateStatusMessage = "Analyzing your sample data, schema will load momentarily ..."
    }

    // a bit tricky ...
    // so users can upload a file or paste in docs
    // if they upload a file containing a small (<15K) of text data, then we'll show it in the textarea
    // they can change the text and re-analyze too
    // if no changes or nothing uploaded, the server-side uses the latest sample data stored in the blob store

    if ($scope.fileUpload) {
      var file = $scope.fileUpload;
      var fd = new FormData();
      fd.append('file', file);
      SchemaDesigner.upload(params, fd, function (data) {
        if (data.sampleDocuments) {
          $scope.sampleDocuments = data.sampleDocuments;
        }
        if (data.numDocs > 0) {
          delete $scope.fileUpload; // docs are on the server ... we don't need to keep uploading them
        }
        $scope.onSchemaUpdated(schema, data, nodeId);
      }, $scope.errorHandler);
    } else {
      // don't need to keep re-posting the same sample if already stored in the blob store
      var postData = null;
      if (!$scope.hasDocsOnServer) {
        postData = $scope.sampleDocuments;
        if (!postData && !$scope.published) {
          return;
        }
      }

      var respHandler = function (data) {
        if (data.sampleDocuments) {
          $scope.sampleDocuments = data.sampleDocuments;
        }
        $scope.onSchemaUpdated(schema, data, nodeId);
      };

      // TODO: need a better approach to detecting the content type from text content
      var contentType = "text/plain";
      if (postData != null) {
        var txt = postData.trim();
        if ((txt.startsWith("[") && txt.includes("]")) || (txt.startsWith("{") && txt.includes("}"))) {
          contentType = "application/json"
        } else if (txt.startsWith("<") || txt.includes("<add>") || txt.includes("<!--")) {
          contentType = "text/xml";
        } else {
          contentType = "application/csv";
        }
      }

      if (contentType === "text/xml") {
        SchemaDesigner.postXml(params, postData, respHandler, $scope.errorHandler);
      } else if (contentType === "application/csv") {
        SchemaDesigner.postCsv(params, postData, respHandler, $scope.errorHandler);
      } else {
        SchemaDesigner.post(params, postData, respHandler, $scope.errorHandler);
      }
    }
  };

  $scope.onFieldTypeChanged = function () {

    var copyFromType = ["stored", "indexed", "multiValued", "docValues", "useDocValuesAsStored", "tokenized", "uninvertible", "termVectors", "termPositions", "termOffsets",
        "omitNorms", "omitTermFreqAndPositions", "omitPositions", "storeOffsetsWithPositions"];

    var type = $scope.selectedNode.type

    // when the user updates the selected field's type, we go apply the
    // new field type's properties to the selected node
    for (var i in $scope.fieldTypes) {
      if ($scope.fieldTypes[i].text == type) {
        var ft = $scope.fieldTypes[i];
        $scope.selectedNode.fieldType = ft;
        for (var i in copyFromType) {
          var x = copyFromType[i];
          if (ft.a_attr[x] !== null) {
            $scope.selectedNode[x] = ft.a_attr[x];
          } else {
            delete $scope.selectedNode[x];
          }
        }
        $scope.selectedUpdated = true;
        break;
      }
    }

    $scope.selectedNode = applyConstraintsOnField($scope.selectedNode);
    if ($scope.selectedUpdated) {
      // for luke analysis, we need the type info here, not the specific field into b/c it just changed.
      $scope.initTypeAnalysisInfo(type, "type");
    }
  };

  $scope.isDisabled = function (dep) {
    if (!$scope.selectedNode) {
      return false;
    }

    if (dep === "termVectors") {
      // termVectors dependency depends on indexed
      return !($scope.selectedNode.indexed && $scope.selectedNode.termVectors);
    }

    if (dep === "not-text" || dep === "docValues") {
      if ($scope.selectedNode.fieldType && $scope.selectedNode.fieldType.a_attr.class === "solr.TextField") {
        // no doc values for TextField
        return true;
      }
    }

    return $scope.selectedNode[dep] === false;
  };

  // this updates checkboxes based on the current settings
  $scope.markSelectedUpdated = function (event) {
    delete $scope.updateStatusMessage;
    $scope.selectedUpdated = true; // enable the update button for this field
  };

  $scope.deleteSelected = function () {
    // console.log(">> deleteSelected");
  };

  $scope.updateSelected = function () {
    if (!$scope.selectedNode) {

      if ($scope.selectedUpdated) {
        // some root level property changed ... re-analyze
        $scope.doAnalyze("/");
      }

      return;
    }

    delete $scope.updateSelectedError;

    // make a copy for the PUT
    var href = $scope.selectedNode.href;
    var id = $scope.selectedNode.id;

    var putData = JSON.parse(JSON.stringify($scope.selectedNode));
    if ($scope.selectedType === "Field" && putData.copyDest) {
      var fields = putData.copyDest.split(",");
      for (var f in fields) {
        var name = fields[f].trim();
        if (!$scope.fieldNames.includes(name)) {
          $scope.updateSelectedError = "Copy to field '"+name+"' doesn't exist!";
          return;
        }
        if (name === $scope.selectedNode.name) {
          $scope.updateSelectedError = "Cannot copy a field to itself!";
          return;
        }
      }
    } else {
      delete putData.copyDest;
    }

    delete putData.fieldType;
    delete putData.href;
    delete putData.id;

    putData = applyConstraintsOnField(putData);

    $scope.updateWorking = true;
    $scope.updateStatusMessage = "Updating "+$scope.selectedType+" ...";

    SchemaDesigner.put({path: "update", configSet: $scope.currentSchema, schemaVersion: $scope.schemaVersion}, putData, function (data) {
      $scope.schemaVersion = data.schemaVersion;
      $scope.currentSchema = data.configSet;
      $scope.core = data.core;

      var nodeType = data.updateType;
      $scope.selectedNode = data[nodeType];
      $scope.selectedNode.href = href;
      $scope.selectedNode.id = id;

      var name = nodeType === "field" ? $scope.selectedNode.type : $scope.selectedNode.name;
      $scope.initTypeAnalysisInfo(name, "type");
      $scope.showFieldDetails = true;

      if (nodeType === "field" && $scope.selectedNode.tokenized) {
        $scope.showAnalysis = true;
        $scope.updateSampleDocId();
      }

      $scope.onSchemaUpdated($scope.currentSchema, data, href);
    }, $scope.errorHandler);
  };

  // TODO: These are copied from analysis.js, so move to a shared location for both vs. duplicating
  var getShortComponentName = function (longname) {
    var short = -1 !== longname.indexOf('$')
        ? longname.split('$')[1]
        : longname.split('.').pop();
    return short.match(/[A-Z]/g).join('');
  };

  var getCaptionsForComponent = function (data) {
    var captions = [];
    for (var key in data[0]) {
      key = key.replace(/.*#/, '');
      if (key != "match" && key != "positionHistory") {
        captions.push(key.replace(/.*#/, ''));
      }
    }
    return captions;
  };

  var getTokensForComponent = function (data) {
    var tokens = [];
    var previousPosition = 0;
    var index = 0;
    for (var i in data) {
      var tokenhash = data[i];
      var positionDifference = tokenhash.position - previousPosition;
      for (var j = positionDifference; j > 1; j--) {
        tokens.push({position: tokenhash.position - j + 1, blank: true, index: index++});
      }

      var token = {position: tokenhash.position, keys: [], index: index++};

      for (key in tokenhash) {
        if (key == "match" || key == "positionHistory") {
          //skip, to not display these keys in the UI
        } else {
          var tokenInfo = new Object();
          tokenInfo.name = key;
          tokenInfo.value = tokenhash[key];
          if ('text' === key || 'raw_bytes' === key) {
            if (tokenhash.match) {
              tokenInfo.extraclass = 'match'; //to highlight matching text strings
            }
          }
          token.keys.push(tokenInfo);
        }
      }
      tokens.push(token);
      previousPosition = tokenhash.position;
    }
    return tokens;
  };

  var extractComponents = function (data, result, name) {
    if (data) {
      result[name] = [];
      for (var i = 0; i < data.length; i += 2) {
        var component = {
          name: data[i],
          short: getShortComponentName(data[i]),
          captions: getCaptionsForComponent(data[i + 1]),
          tokens: getTokensForComponent(data[i + 1])
        };
        result[name].push(component);
      }
    }
  };

  var processFieldAnalysisData = function (analysis) {
    var response = {};
    extractComponents(analysis.index, response, "index");
    return response;
  };

  $scope.doPublish = function () {
    var params = {
      path: "publish",
      configSet: $scope.currentSchema,
      schemaVersion: $scope.schemaVersion,
      reloadCollections: $scope.reloadOnPublish,
      cleanupTemp: true
    };
    if ($scope.newCollection && $scope.newCollection.name) {
      params.newCollection = $scope.newCollection.name;
      params.numShards = $scope.newCollection.numShards;
      params.replicationFactor = $scope.newCollection.replicationFactor;
      params.indexToCollection = $scope.newCollection.indexToCollection;
    }
    SchemaDesigner.put(params, null, function (data) {
      $scope.schemaVersion = data.schemaVersion;
      $scope.currentSchema = data.configSet;

      delete $scope.selectedNode;
      delete $scope.currentSchema;
      delete $scope.newSchema;
      $scope.showPublish = false;
      $scope.refresh();

      if (data.newCollection) {
        $window.location.href = "#/" + data.newCollection + "/collection-overview";
      }
    }, $scope.errorHandler);
  };

  $scope.downloadConfig = function () {
    location.href = "/api/schema-designer/download?wt=raw&configSet="+$scope.currentSchema;
  };

  function docsToTree(docs) {
    var children = [];
    for (var i in docs) {
      var id = docs[i][$scope.uniqueKeyField];
      if (!id) {
        id = "" + i; // id not in results so use the position in results as the value
      }
      var nodeId = "doc/" + id;
      docs[i].href = nodeId;
      children.push({"text": id, "a_attr": docs[i], "id": nodeId});
    }
    return children;
  }

  function debugToTree(debugObj) {
    var children = [];
    for (var x in debugObj) {
      if (typeof debugObj[x] === 'object') {
        var obj = debugObj[x];
        var nodeId = "debug/" + x;
        var tdata = [];
        for (var a in obj) {
          if (typeof obj[a] !== 'object') {
            tdata.push({name:a,value:obj[a]});
          }
        }
        children.push({"text": x, "a_attr": {"href": nodeId}, "tdata":tdata, "id": nodeId, "children": []});
      }
    }
    return children;
  }

  function facetsToTree(ff) {
    var children = [];
    for (var f in ff) {
      var nodeId = "facet/" + f;
      if (ff[f] && ff[f].length > 0) {
        var facet = ff[f];
        var tdata = [];
        for (let i = 0; i < facet.length; i += 2) {
          tdata.push({name:facet[i],value:facet[i+1]});
        }
        children.push({"text": f, "a_attr": {"href": nodeId}, "tdata":tdata, "id": nodeId, "children": []});
      }
    }
    return children;
  }

  $scope.selectField = function(event) {
    var t = event.target || event.srcElement || event.currentTarget;
    if (t && t.text) {
      var nodeId = "field/"+t.text;
      $scope.onSelectSchemaTreeNode(nodeId);
      // TODO: the node doesn't get selected in the tree, but calling jst.select_node leads to jstree errors
      var jst = $('#schemaJsTree').jstree();
      var node = jst.get_node(nodeId);
      if (node) {
        var selected_node = jst.get_selected();
        if (selected_node) {
          jst.deselect_node(selected_node);
        }
        jst.select_node(nodeId, true);
      }
    }
  };

  $scope.renderResultsTree = function (data) {
    var h = data.responseHeader;
    var sort = h.params.sort;
    if (!sort) {
      sort = "score desc";
    }

    $scope.resultsMeta = [
      {name:"Query",value:h.params.q},
      {name:"QTime",value:h.QTime},
      {name:"Hits",value:data.response.numFound},
      {name:"sort",value:sort},
    ];

    var excParams = ["q", "handler", "debug", "configSet", "wt", "version", "_", "sort"];
    for (var p in h.params) {
      if (!excParams.includes(p)) {
        $scope.resultsMeta.push({name:p,value:h.params[p]});
      }
    }

    $scope.debugMeta = [];
    for (var d in data.debug) {
      if (typeof data.debug[d] !== 'object') {
        var nvp = {name:d,value:data.debug[d]};
        $scope.debugMeta.push(nvp);
        $scope.resultsMeta.push(nvp);
      }
    }

    var rootChildren = [{"text": "Documents", "state": {"opened": true}, "a_attr": {"href": "docs"}, "children": docsToTree(data.response.docs)}];
    if (data.facet_counts && data.facet_counts.facet_fields) {
      rootChildren.push({"text": "Facets", "state": {"opened": true}, "a_attr": {"href": "facets"}, "children": facetsToTree(data.facet_counts.facet_fields)});
    }
    rootChildren.push({"text": "Debug", "a_attr": {"href": "debug"}, "children": debugToTree(data.debug)});
    var tree = [{"text": "Results", "a_attr": {"href": "/"}, "children": rootChildren}];
    $scope.queryResultsTree = tree;
  };

  $scope.onSelectQueryResultsNode = function (id) {
    if (id === "/" || id === "docs") {
      $scope.resultsData = $scope.resultsMeta;
      return;
    }

    if (id === "debug") {
      $scope.resultsData = $scope.debugMeta;
      return;
    }

    var jst = $('#queryResultsJsTree').jstree();
    var node = jst.get_node(id);
    if (!node || !node.a_attr) {
      return;
    }

    if (node.original && node.original.tdata) {
      $scope.resultsData = node.original.tdata;
    } else {
      $scope.resultsData = [];
      for (var a in node.a_attr) {
        if (a === "href") continue;
        var row = {name: a, value: node.a_attr[a]};
        if (id.startsWith("doc/")) {
          row.type = "f"; // so we can link to fields in the schema tree from results!
        }

        $scope.resultsData.push(row);
      }
    }

    if (id.startsWith("doc/")) {
      $scope.sampleDocId = id.substring(4);
      $scope.updateSampleDocId();
    }
  };

  $scope.doQuery = function () {

    var params = {path: "query", configSet: $scope.currentSchema, debug: "true", "wt": "json"};

    if ($scope.selectedFacets && $scope.selectedFacets.length > 0) {
      params["facet"] = true;
      params["facet.field"] = $scope.selectedFacets;
      params["facet.limit"] = 20;
      params["facet.mincount"] = 1;
    } else {
      params["facet"] = false;
      delete params["facet.field"];
    }

    var set = function (key, value) {
      if (params[key]) {
        params[key].push(value);
      } else {
        params[key] = [value];
      }
    }

    params["sort"] = $scope.query.sortBy + " " + $scope.query.sortDir;
    params["q"] = $scope.query.q;

    if ($scope.rawParams) {
      var rawParams = $scope.rawParams.split(/[&\n]/);
      for (var i in rawParams) {
        var param = rawParams[i];
        var equalPos = param.indexOf("=");
        if (equalPos > -1) {
          set(param.substring(0, equalPos), param.substring(equalPos + 1));
        } else {
          set(param, ""); // Use empty value for params without "="
        }
      }
    }

    var qt = params["qt"] ? params["qt"] : "/select";
    if (qt[0] === '/') {
      params.handler = qt.substring(1);
    } else { // Support legacy style handleSelect=true configs
      params.handler = "select";
      params["qt"] = qt;
    }

    SchemaDesigner.get(params, function (data) {
      $scope.renderResultsTree(data);
      $scope.onSelectQueryResultsNode("/");
    });
  };

  $scope.refresh();
})
