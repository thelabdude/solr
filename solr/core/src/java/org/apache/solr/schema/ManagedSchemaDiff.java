package org.apache.solr.schema;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for comparing managed index schemas
 */
public class ManagedSchemaDiff {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String UPDATED_KEY_STRING = "updated";
  private static final String ADDED_KEY_STRING = "added";
  private static final String REMOVED_KEY_STRING = "removed";

  private static final String FIELDS_KEY_STRING = "fields";
  private static final String FIELD_TYPES_KEY_STRING = "fieldTypes";
  private static final String DYNAMIC_FIELDS_KEY_STRING = "dynamicFields";
  private static final String COPY_FIELDS_KEY_STRING = "copyFields";

  public static Map<String, Object> diff(ManagedIndexSchema oldSchema, ManagedIndexSchema newSchema) {
    Map<String, Object> diff = new HashMap<>();

    Map<String, Object> fieldsDiff = diff(mapFieldsToPropertyValues(oldSchema.getFields()), mapFieldsToPropertyValues(newSchema.getFields()));
    Map<String, Object> fieldTypesDiff = diff(mapFieldTypesToPropValues(oldSchema.getFieldTypes()), mapFieldTypesToPropValues(newSchema.getFieldTypes()));
    Map<String, Object> dynamicFieldDiff = diff(mapDynamicFieldToPropValues(oldSchema.getDynamicFields()), mapDynamicFieldToPropValues(newSchema.getDynamicFields()));
    Map<String, Object> copyFieldDiff = diff(getCopyFieldList(oldSchema), getCopyFieldList(newSchema));

    if (!fieldsDiff.isEmpty()) {
      diff.put(FIELDS_KEY_STRING, fieldsDiff);
    }
    if (!fieldTypesDiff.isEmpty()) {
      diff.put(FIELD_TYPES_KEY_STRING, fieldTypesDiff);
    }
    if (!dynamicFieldDiff.isEmpty()) {
      diff.put(DYNAMIC_FIELDS_KEY_STRING, dynamicFieldDiff);
    }
    if (!copyFieldDiff.isEmpty()) {
      diff.put(COPY_FIELDS_KEY_STRING, copyFieldDiff);
    }

    return diff;
  }

  protected static Map<String, Object> diff(Map<String, SimpleOrderedMap<Object>> map1, Map<String, SimpleOrderedMap<Object>> map2) {
    Map<String, List<Map<String, Object>>> changedValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> newValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> removedValues = new HashMap<>();
    for (String fieldName : map1.keySet()) {
      if (map2.containsKey(fieldName)) {
        SimpleOrderedMap<Object> oldPropValues = map1.get(fieldName);
        SimpleOrderedMap<Object> newPropValues = map2.get(fieldName);
        if (!oldPropValues.equals(newPropValues)) {
          List<Map<String, Object>> mapDiff = getMapDifference(oldPropValues, newPropValues);
          if (!mapDiff.isEmpty()) {
            changedValues.put(fieldName, mapDiff);
          }
        }
      } else {
        removedValues.put(fieldName, map1.get(fieldName));
      }
    }

    for (String fieldName : map2.keySet()) {
      if (!map1.containsKey(fieldName)) {
        newValues.put(fieldName, map2.get(fieldName));
      }
    }

    Map<String, Object> mapDiff = new HashMap<>();
    if (!changedValues.isEmpty()) {
      mapDiff.put(UPDATED_KEY_STRING, changedValues);
    }
    if (!newValues.isEmpty()) {
      mapDiff.put(ADDED_KEY_STRING, newValues);
    }
    if (!removedValues.isEmpty()) {
      mapDiff.put(REMOVED_KEY_STRING, removedValues);
    }

    return mapDiff;
  }

  /**
   * TODO:
   * @param simpleOrderedMap1 TODO
   * @param simpleOrderedMap2 TODO
   * @return TODO
   */
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> getMapDifference(
      SimpleOrderedMap<Object> simpleOrderedMap1,
      SimpleOrderedMap<Object> simpleOrderedMap2) {
    Map<String, Object> map1 = simpleOrderedMap1.toMap(new HashMap<>());
    Map<String, Object> map2 = simpleOrderedMap2.toMap(new HashMap<>());
    Map<String, MapDifference.ValueDifference<Object>> mapDiff = Maps.difference(map1, map2).entriesDiffering();
    if (mapDiff.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, Object> leftMapDiff =
        mapDiff.entrySet().stream()
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().leftValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<String, Object> rightMapDiff =
        mapDiff.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), entry.getValue().rightValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return Arrays.asList(leftMapDiff, rightMapDiff);
  }

  protected static Map<String, Object> diff(List<SimpleOrderedMap<Object>> list1, List<SimpleOrderedMap<Object>> list2) {
    List<SimpleOrderedMap<Object>> oldList = new ArrayList<>(); // ordered map changed in list1 compared to list2
    List<SimpleOrderedMap<Object>> newList = new ArrayList<>(); // ordered map changed in list2 compared to list1

    list1.forEach(som -> {
      if (!list2.contains(som)) {
        oldList.add(som);
      }
    });

    list2.forEach(som -> {
      if (!list1.contains(som)) {
        newList.add(som);
      }
    });

    Map<String, Object> mapDiff = new HashMap<>();
    if (!oldList.isEmpty()) {
      mapDiff.put("old", oldList);
    }
    if (!newList.isEmpty()) {
      mapDiff.put("new", newList);
    }
    return mapDiff;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldsToPropertyValues(Map<String, SchemaField> fields) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fields.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(true)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldTypesToPropValues(Map<String, FieldType> fieldTypes) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fieldTypes.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(true)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapDynamicFieldToPropValues(IndexSchema.DynamicField[] dynamicFields) {
    return
        Stream.of(dynamicFields)
            .map(df -> Map.entry(df.getPrototype().getName(), df.getPrototype().getNamedPropertyValues(true)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected static List<SimpleOrderedMap<Object>> getCopyFieldList(ManagedIndexSchema indexSchema) {
    return indexSchema.getCopyFieldProperties(false, null, null);
  }
}
