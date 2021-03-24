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

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.SafeXMLParsing;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;

public class DefaultSampleDocumentsLoader implements SampleDocumentsLoader {
  public static final String CSV_MULTI_VALUE_DELIM_PARAM = "csvMultiValueDelimiter";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static byte[] streamAsBytes(final InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int r;
    try (in) {
      while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
    }
    return baos.toByteArray();
  }

  @Override
  public SampleDocuments load(SolrParams params, ContentStream stream, final int maxDocsToLoad) throws IOException {
    final String contentType = stream.getContentType();
    if (contentType == null) {
      return SampleDocuments.NONE;
    }

    if (params == null) {
      params = new ModifiableSolrParams();
    }

    String fileSource = null;
    byte[] uploadedBytes = null;
    if ("file".equals(stream.getName())) {
      fileSource = stream.getSourceInfo() != null ? stream.getSourceInfo() : "file";
      uploadedBytes = streamAsBytes(stream.getStream());
      if (uploadedBytes.length > (10 * 1024 * 1024)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            fileSource + " is too big! 10MB is the max upload size for sample documents.");
      }
      // capture the bytes for this file upload as we want to use them for showing the contents in the textarea
      stream = new ContentStreamBase.ByteArrayStream(uploadedBytes, fileSource, contentType);
    }

    List<SolrInputDocument> docs = null;
    if (stream.getSize() > 0) {
      if (contentType.contains(JSON_MIME)) {
        docs = loadJsonDocs(params, stream, maxDocsToLoad);
      } else if (contentType.contains("text/xml") || contentType.contains("application/xml")) {
        docs = loadXmlDocs(params, stream, maxDocsToLoad);
      } else if (contentType.contains("text/csv")) {
        docs = loadCsvDocs(params, stream, maxDocsToLoad);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, contentType + " not supported yet!");
      }

      if (docs != null && maxDocsToLoad > 0 && docs.size() > maxDocsToLoad) {
        docs = docs.subList(0, maxDocsToLoad);
      }
    }

    return new SampleDocuments(docs, contentType, fileSource, uploadedBytes);
  }

  protected List<SolrInputDocument> loadCsvDocs(SolrParams params, ContentStream stream, final int maxDocsToLoad) throws IOException {
    SampleCSVLoader csvLoader = new SampleCSVLoader(new CSVRequest(params), maxDocsToLoad);
    return csvLoader.loadDocs(stream);
  }

  @SuppressWarnings("unchecked")
  protected List<SolrInputDocument> loadJsonDocs(SolrParams params, ContentStream stream, final int maxDocsToLoad) throws IOException {
    final Object json;
    try (Reader reader = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(reader));
    }
    if (json == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected at least 1 JSON doc in the request body!");
    }
    List<Map<String, Object>> docs;
    if (json instanceof List) {
      // list of docs
      docs = (List<Map<String, Object>>) json;
    } else if (json instanceof Map) {
      // single doc ...
      // TODO: try to find the split path by looking for the first path the results in multiple docs
      docs = Collections.singletonList((Map<String, Object>) json);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected one or more JSON docs in the request body!");
    }
    if (maxDocsToLoad > 0 && docs.size() > maxDocsToLoad) {
      docs = docs.subList(0, maxDocsToLoad);
    }
    return docs.stream().map(JsonLoader::buildDoc).collect(Collectors.toList());
  }

  protected List<SolrInputDocument> loadXmlDocs(SolrParams params, ContentStream stream, final int maxDocsToLoad) throws IOException {
    String xmlString = readInputAsString(stream.getStream()).trim();
    List<SolrInputDocument> docs = null;
    if (xmlString.contains("<add>") && xmlString.contains("<doc>")) {
      XMLInputFactory inputFactory = XMLInputFactory.newInstance();
      XMLStreamReader parser = null;
      try {
        parser = inputFactory.createXMLStreamReader(new StringReader(xmlString));
        docs = parseXmlDocs(parser, maxDocsToLoad);
      } catch (XMLStreamException e) {
        throw new IOException(e);
      } finally {
        if (parser != null) {
          try {
            parser.close();
          } catch (XMLStreamException ignore) {
          }
        }
      }
    } else {
      Document xmlDoc;
      try {
        xmlDoc = SafeXMLParsing.parseUntrustedXML(log, xmlString);
      } catch (SAXException e) {
        throw new IOException(e);
      }
      Element root = xmlDoc.getDocumentElement();
      // TODO: support other types of XML here
      throw new IOException("TODO: XML documents with root " + root.getTagName() + " not supported yet!");
    }
    return docs;
  }

  protected List<SolrInputDocument> parseXmlDocs(XMLStreamReader parser, final int maxDocsToLoad) throws XMLStreamException {
    List<SolrInputDocument> docs = new LinkedList<>();
    XMLLoader loader = new XMLLoader().init(null);
    while (true) {
      final int event;
      try {
        event = parser.next();
      } catch (java.util.NoSuchElementException noSuchElementException) {
        return docs;
      }
      switch (event) {
        case XMLStreamConstants.END_DOCUMENT:
          parser.close();
          return docs;
        case XMLStreamConstants.START_ELEMENT:
          if ("doc".equals(parser.getLocalName())) {
            SolrInputDocument doc = loader.readDoc(parser);
            if (doc != null) {
              docs.add(doc);

              if (maxDocsToLoad > 0 && docs.size() >= maxDocsToLoad) {
                parser.close();
                return docs;
              }
            }
          }
      }
    }
  }

  protected String readInputAsString(InputStream in) throws IOException {
    return new String(streamAsBytes(in), StandardCharsets.UTF_8);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void init(NamedList args) {

  }

  private static class NoOpUpdateRequestProcessor extends UpdateRequestProcessor {
    NoOpUpdateRequestProcessor() {
      super(null);
    }
  }

  private static class CSVRequest extends SolrQueryRequestBase {
    CSVRequest(SolrParams params) {
      super(null, params);
    }
  }

  private class SampleCSVLoader extends CSVLoaderBase {
    List<SolrInputDocument> docs = new LinkedList<>();
    CSVRequest req;
    int maxDocsToLoad;
    String multiValueDelimiter;

    SampleCSVLoader(CSVRequest req, int maxDocsToLoad) {
      super(req, new NoOpUpdateRequestProcessor());
      this.req = req;
      this.maxDocsToLoad = maxDocsToLoad;
      this.multiValueDelimiter = req.getParams().get(CSV_MULTI_VALUE_DELIM_PARAM);
    }

    List<SolrInputDocument> loadDocs(ContentStream stream) throws IOException {
      load(req, new SolrQueryResponse(), stream, processor);
      return docs;
    }

    @Override
    void addDoc(int line, String[] vals) throws IOException {
      if (maxDocsToLoad > 0 && docs.size() >= maxDocsToLoad) {
        return; // just a short circuit, probably doesn't help that much
      }

      templateAdd.clear();
      SolrInputDocument doc = new SolrInputDocument();
      doAdd(line, vals, doc, templateAdd);
      if (templateAdd.solrDoc != null) {
        if (multiValueDelimiter != null) {
          for (SolrInputField field : templateAdd.solrDoc.values()) {
            if (field.getValueCount() == 1) {
              Object value = field.getFirstValue();
              if (value instanceof String) {
                String[] splitValue = ((String) value).split(multiValueDelimiter);
                if (splitValue.length > 1) {
                  field.setValue(Arrays.asList(splitValue));
                }
              }
            }
          }
        }
        docs.add(templateAdd.solrDoc);
      }
    }
  }
}
