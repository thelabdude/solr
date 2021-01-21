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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

/**
 * Map a source field to a different field in the to-be indexed document.
 *
 */
public class FieldMappingUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private final List<FieldMapping> sortedFieldMappings = new ArrayList<>();

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        final SolrInputDocument doc = cmd.getSolrInputDocument();
        sortedFieldMappings.forEach(m -> m.applyMapping(doc));
        super.processAdd(cmd);
      }

      @Override
      public void processDelete(DeleteUpdateCommand cmd) throws IOException {
        super.processDelete(cmd);
      }
    };
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void init(NamedList args) {
    List<NamedList> mappings = (List<NamedList>) args.get("mappings");
    if (mappings == null || mappings.size() == 0) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Expected 'mappings' param in config for " + this.getClass().getName());
    }

    for (NamedList next : mappings) {
      String input = (String) next.get("input");
      if (input == null || input.trim().isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Each field mapping must define an input field! " + next);
      }

      String op = (String) next.get("op");
      MappingOp mappingOp = (op == null || op.trim().isEmpty()) ? MappingOp.rename : MappingOp.valueOf(op.trim().toLowerCase(Locale.ROOT));

      String output = (String) next.get("output");
      String outputField = output != null && !output.trim().isEmpty() ? output.trim() : null;
      if (outputField == null && (mappingOp == MappingOp.rename || mappingOp == MappingOp.copy)) {
        // mapping must be remove if there is no output field defined
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Each field mapping must define an output field for " + mappingOp + "; " + next);
      }

      Pattern searchRegex = null;
      String regex = (String) next.get("searchRegex");
      if (regex != null) {
        searchRegex = Pattern.compile(regex);
      }

      if (searchRegex == null && mappingOp == MappingOp.replace_value) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Must provide a 'searchRegex' param for 'replace_value' mappings; " + next);
      }

      String replace = (String) next.get("replace");
      if (searchRegex != null && replace == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Must provide a 'replace' param when defining a search regex; " + next);
      }

      sortedFieldMappings.add(new FieldMapping(input.trim(), mappingOp, outputField, searchRegex, replace));
    }

    // make sure remove operations happen first
    Collections.sort(sortedFieldMappings);

    super.init(args);
  }

  private enum MappingOp {
    remove, replace_value, rename, copy
  }

  private static class FieldMapping implements Comparable<FieldMapping> {
    final String inputField;
    final MappingOp mapping;
    final String outputField;
    final Pattern searchRegex;
    final String replace;

    FieldMapping(String inputField, MappingOp mapping, String outputField, Pattern searchRegex, String replace) {
      this.inputField = inputField;
      this.mapping = mapping;
      this.outputField = outputField;
      this.searchRegex = searchRegex;
      this.replace = replace;
    }

    void applyMapping(SolrInputDocument doc) {
      SolrInputField input = doc.getField(inputField);
      if (input != null) {
        if (mapping == MappingOp.rename) {
          input = doc.remove(inputField);
          input.setName(outputField);
          doc.put(outputField, applySearchAndReplace(input));
        } else if (mapping == MappingOp.copy) {
          doc.remove(outputField);
          doc.put(outputField, applySearchAndReplace(input.deepCopy()));
        } else if (mapping == MappingOp.replace_value) {
          doc.remove(inputField);
          doc.put(inputField, applySearchAndReplace(input));
        } else {
          doc.remove(inputField);
        }
      }
    }

    SolrInputField applySearchAndReplace(SolrInputField input) {
      if (searchRegex != null) {
        // TODO: implement this

      }
      return input;
    }

    @Override
    public int compareTo(FieldMapping o) {
      // order is by mapping op and then by input field name asc
      final int byMapping = this.mapping.compareTo(o.mapping);
      return (byMapping == 0) ? this.inputField.compareTo(o.inputField) : byMapping;
    }
  }
}
