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

package org.apache.solr.handler.loader;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;

public class SampleDocuments {
  public static final SampleDocuments NONE = new SampleDocuments(null, null, null, null);

  public List<SolrInputDocument> parsed;
  public final String contentType;
  public final byte[] uploadedBytes;
  public final String fileSource;

  public SampleDocuments(List<SolrInputDocument> parsed, String contentType, String fileSource, byte[] uploadedBytes) {
    this.parsed = parsed != null ? parsed : new LinkedList<>(); // needs to be mutable
    this.contentType = contentType;
    this.fileSource = fileSource;
    this.uploadedBytes = uploadedBytes;
  }

  private boolean isTextContentType() {
    if (contentType == null) {
      return false;
    }
    return contentType.contains(JSON_MIME) || contentType.startsWith("text/") || contentType.contains("application/xml");
  }

  public String getSampleText() {
    String text = null;
    if (uploadedBytes != null && isTextContentType()) {
      text = new String(uploadedBytes, StandardCharsets.UTF_8);
      // 20K limit on the sample text document view
      if (text.length() > (20 * 1024)) {
        text = null; // too big, don't show sample in textarea on UI
      }
    }
    return text;
  }

  public List<SolrInputDocument> appendDocs(List<SolrInputDocument> add, int maxDocsToLoad) {
    if (add != null && !add.isEmpty()) {
      parsed.addAll(add);
      if (maxDocsToLoad > 0 && parsed.size() > maxDocsToLoad) {
        parsed = parsed.subList(0, maxDocsToLoad);
      }
    }
    return parsed;
  }
}
