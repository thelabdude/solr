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

package org.apache.solr.schema;

import java.lang.invoke.MethodHandles;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.LocaleUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.update.processor.ParseBooleanFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseDateFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseDoubleFieldUpdateProcessorFactory;
import org.apache.solr.update.processor.ParseLongFieldUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.ParseDateFieldUpdateProcessorFactory.validateFormatter;

// Just a quick hack to flush out the design, more intelligence is needed
public class DefaultSchemaSuggester implements SchemaSuggester {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final List<String> DEFAULT_DATE_TIME_PATTERNS =
      Arrays.asList("yyyy-MM-dd['T'[HH:mm[:ss[.SSS]][z", "yyyy-MM-dd['T'[HH:mm[:ss[,SSS]][z", "yyyy-MM-dd HH:mm[:ss[.SSS]][z", "yyyy-MM-dd HH:mm[:ss[,SSS]][z", "[EEE, ]dd MMM yyyy HH:mm[:ss] z", "EEEE, dd-MMM-yy HH:mm:ss z", "EEE MMM ppd HH:mm:ss [z ]yyyy");

  private static final String FORMATS_PARAM = "format";
  private static final String DEFAULT_TIME_ZONE_PARAM = "defaultTimeZone";
  private static final String LOCALE_PARAM = "locale";
  private static final String TRUE_VALUES_PARAM = "trueValue";
  private static final String FALSE_VALUES_PARAM = "falseValue";
  private static final String CASE_SENSITIVE_PARAM = "caseSensitive";
  // boolean parsing
  private final Set<String> trueValues = new HashSet<>(Arrays.asList("true"));
  private final Set<String> falseValues = new HashSet<>(Arrays.asList("false"));
  private final List<DateTimeFormatter> dateTimeFormatters = new LinkedList<>();
  private boolean caseSensitive = false;

  @Override
  public Optional<SchemaField> suggestField(String fieldName, List<Object> sampleValues, IndexSchema schema, List<String> langs) {

    // start by looking at the fieldName and seeing if there is a dynamic field in the schema that already applies
    if (schema.isDynamicField(fieldName)) {
      return Optional.of(schema.getFieldOrNull(fieldName));
    }

    // TODO: use passed in langs
    Locale locale = Locale.ENGLISH;

    String fieldTypeName = guessFieldType(fieldName, sampleValues, schema, locale);
    FieldType fieldType = schema.getFieldTypeByName(fieldTypeName);
    if (fieldType == null) {
      // TODO: construct this field type on-the-fly ...
      throw new IllegalStateException("FieldType '" + fieldTypeName + "' not found in the schema!");
    }

    Map<String, String> fieldProps = guessFieldProps(fieldName, fieldType, sampleValues, schema);
    SchemaField schemaField = schema.newField(fieldName, fieldTypeName, fieldProps);
    return Optional.of(schemaField);
  }

  @Override
  public ManagedIndexSchema adaptExistingFieldToData(SchemaField schemaField, List<Object> sampleValues, ManagedIndexSchema schema) {
    // Promote a single-valued to multi-valued if needed
    if (!schemaField.multiValued()) {
      if (isMultiValued(sampleValues)) {
        // this existing field needs to be promoted to multi-valued
        SimpleOrderedMap<Object> fieldProps = schemaField.getNamedPropertyValues(false);
        fieldProps.add("multiValued", true);
        fieldProps.remove("name");
        fieldProps.remove("type");
        schema = schema.replaceField(schemaField.name, schemaField.type, fieldProps.asShallowMap());
      }
    }
    // TODO: other "healing" type operations here ...
    return schema;
  }

  @Override
  public Map<String, List<Object>> transposeDocs(List<SolrInputDocument> docs) {
    Map<String, List<Object>> mapByField = new HashMap<>();
    docs.forEach(doc -> doc.getFieldNames().forEach(f -> {
      List<Object> values = mapByField.computeIfAbsent(f, k -> new LinkedList<>());
      Collection<Object> fieldValues = doc.getFieldValues(f);
      if (fieldValues != null && !fieldValues.isEmpty()) {
        if (fieldValues.size() == 1) {
          // flatten so every field doesn't end up multi-valued
          values.add(fieldValues.iterator().next());
        } else {
          // truly multi-valued
          values.add(fieldValues);
        }
      }
    }));
    return mapByField;
  }

  protected String guessFieldType(String fieldName, final List<Object> sampleValues, IndexSchema schema, Locale locale) {
    String type = null;
    boolean isMV = isMultiValued(sampleValues);

    // flatten values to a single stream for easier analysis; also remove nulls
    List<Object> flattened = sampleValues.stream()
        .flatMap(c -> (c instanceof Collection) ? ((Collection<?>) c).stream() : Stream.of(c))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (isBoolean(flattened)) {
      type = isMV ? "booleans" : "boolean";
    } else {
      String intType = isIntOrLong(flattened, locale);
      if (intType != null) {
        type = isMV ? intType + "s" : intType;
      } else {
        String floatType = isFloatOrDouble(flattened, locale);
        if (floatType != null) {
          type = isMV ? floatType + "s" : floatType;
        }
      }
    }

    if (type == null) {
      if (isDateTime(flattened)) {
        type = isMV ? "pdates" : "pdate";
      } else if (isText(flattened)) {
        // text_en is not MV?!?
        type = !isMV && "en".equals(locale.getLanguage()) ? "text_en" : "text_general";
      }
    }

    // if we get here and haven't made a decision, it's a string
    if (type == null) {
      type = isMV ? "strings" : "string";
    }

    return type;
  }

  protected boolean isText(List<Object> values) {
    if (values == null || values.isEmpty()) {
      return false;
    }

    int maxLength = -1;
    int maxTerms = -1;
    for (Object next : values) {
      if (!(next instanceof String)) {
        return false;
      }

      String cs = (String) next;
      int len = cs.length();
      if (len > maxLength) {
        maxLength = len;
      }

      String[] terms = cs.split("\\s+");
      if (terms.length > maxTerms) {
        maxTerms = terms.length;
      }
    }

    // don't want to choose text for fields where string will do
    // if most of the sample values are unique but only a few terms, then it's likely a text field
    return (maxLength > 100 || maxTerms > 15 || (maxTerms > 4 && ((float)Sets.newHashSet(values).size()/values.size()) > 0.9f));
  }

  protected String isFloatOrDouble(List<Object> values, Locale locale) {
    NumberFormat format = NumberFormat.getInstance(locale);
    format.setParseIntegerOnly(false);
    format.setRoundingMode(RoundingMode.CEILING);
    boolean isFloat = true;
    for (Object next : values) {
      Object parsed = ParseDoubleFieldUpdateProcessorFactory.parsePossibleDouble(next, format);
      if (parsed == null) {
        // not a double ...
        return null;
      }

      Number num = (Number) parsed;
      String str = num.toString();
      int dotAt = str.indexOf('.');
      if (dotAt != -1) {
        String scalePart = str.substring(dotAt + 1);
        if (scalePart.length() > 2) {
          isFloat = false;
        }
      }
    }

    return isFloat ? "pfloat" : "pdouble";
  }

  protected boolean isBoolean(List<Object> values) {
    for (Object next : values) {
      Object parsed = ParseBooleanFieldUpdateProcessorFactory.parsePossibleBoolean(next, caseSensitive, trueValues, falseValues);
      if (parsed == null) {
        return false;
      }
    }
    // all values are booleans
    return true;
  }

  protected String isIntOrLong(List<Object> values, Locale locale) {
    NumberFormat format = NumberFormat.getInstance(locale);
    format.setParseIntegerOnly(true);
    long maxLong = Long.MIN_VALUE;
    for (Object next : values) {
      Object parsed = ParseLongFieldUpdateProcessorFactory.parsePossibleLong(next, format);
      if (parsed == null) {
        // not a long ...
        return null;
      } else {
        long parsedLong = ((Number) parsed).longValue();
        if (parsedLong > maxLong) {
          maxLong = parsedLong;
        }
      }
    }

    // if all values are less than some smallish threshold, then it's likely this field holds small numbers
    // but be very conservative here as it's simply an optimization and we can always fall back to long
    return maxLong < 10000 ? "pint" : "plong";
  }

  protected boolean isDateTime(List<Object> values) {
    if (dateTimeFormatters.isEmpty()) {
      return false;
    }

    for (Object next : values) {
      Object parsedDate = ParseDateFieldUpdateProcessorFactory.parsePossibleDate(next, dateTimeFormatters, new ParsePosition(0));
      if (parsedDate == null) {
        // not a date value
        return false;
      }
    }
    return true;
  }

  protected boolean isMultiValued(final List<Object> sampleValues) {
    for (Object next : sampleValues) {
      if (next instanceof Collection) {
        return true;
      }
    }
    return false;
  }

  protected Map<String, String> guessFieldProps(String fieldName, FieldType fieldType, List<Object> sampleValues, IndexSchema schema) {
    Map<String, String> props = new HashMap<>();
    props.put("indexed", "true");

    //props.put("multiValued", "false"); // MV comes from the field type

    boolean docValues = true;
    if (fieldType instanceof TextField) {
      docValues = false;
    } else {
      try {
        fieldType.checkSupportsDocValues();
      } catch (SolrException solrException) {
        docValues = false;
      }
    }

    props.put("docValues", String.valueOf(docValues));

    if (!docValues) {
      props.put("stored", "true");
    } else {
      props.put("stored", "false");
      props.put("useDocValuesAsStored", "true");
    }

    return props;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void init(NamedList args) {
    initDateTimeFormatters(args);
    initBooleanParsing(args);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void initDateTimeFormatters(NamedList args) {

    Locale locale = Locale.US;
    String localeParam = (String) args.remove(LOCALE_PARAM);
    if (null != localeParam) {
      locale = LocaleUtils.toLocale(localeParam);
    }

    ZoneId defaultTimeZone = ZoneOffset.UTC;
    Object defaultTimeZoneParam = args.remove(DEFAULT_TIME_ZONE_PARAM);
    if (null != defaultTimeZoneParam) {
      defaultTimeZone = ZoneId.of(defaultTimeZoneParam.toString());
    }

    Collection<String> dateTimePatterns = args.removeConfigArgs(FORMATS_PARAM);
    if (dateTimePatterns == null || dateTimePatterns.isEmpty()) {
      dateTimePatterns = DEFAULT_DATE_TIME_PATTERNS;
    }

    for (String pattern : dateTimePatterns) {
      DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseLenient().parseCaseInsensitive()
          .appendPattern(pattern).toFormatter(locale).withResolverStyle(ResolverStyle.LENIENT).withZone(defaultTimeZone);
      validateFormatter(formatter);
      dateTimeFormatters.add(formatter);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected void initBooleanParsing(NamedList args) {
    Object caseSensitiveParam = args.remove(CASE_SENSITIVE_PARAM);
    if (null != caseSensitiveParam) {
      if (caseSensitiveParam instanceof Boolean) {
        caseSensitive = (Boolean) caseSensitiveParam;
      } else {
        caseSensitive = Boolean.parseBoolean(caseSensitiveParam.toString());
      }
    }

    Collection<String> trueValuesParam = args.removeConfigArgs(TRUE_VALUES_PARAM);
    if (!trueValuesParam.isEmpty()) {
      trueValues.clear();
      for (String trueVal : trueValuesParam) {
        trueValues.add(caseSensitive ? trueVal : trueVal.toLowerCase(Locale.ROOT));
      }
    }

    Collection<String> falseValuesParam = args.removeConfigArgs(FALSE_VALUES_PARAM);
    if (!falseValuesParam.isEmpty()) {
      falseValues.clear();
      for (String val : falseValuesParam) {
        final String falseVal = caseSensitive ? val : val.toLowerCase(Locale.ROOT);
        if (trueValues.contains(falseVal)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Param '" + FALSE_VALUES_PARAM + "' contains a value also in param '" + TRUE_VALUES_PARAM
                  + "': '" + val + "'");
        }
        falseValues.add(falseVal);
      }
    }
  }
}
