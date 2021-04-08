package org.apache.solr.schema;

import org.apache.solr.common.util.SimpleOrderedMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ManagedSchemaDiff {

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

    diff.put(FIELDS_KEY_STRING, fieldsDiff);
    diff.put(FIELD_TYPES_KEY_STRING, fieldTypesDiff);
    diff.put(DYNAMIC_FIELDS_KEY_STRING, dynamicFieldDiff);
    diff.put(COPY_FIELDS_KEY_STRING, copyFieldDiff);

    return diff;
  }

  protected static Map<String, Object> diff(Map<String, SimpleOrderedMap<Object>> map1, Map<String, SimpleOrderedMap<Object>> map2) {
    Map<String, List<SimpleOrderedMap<Object>>> changedValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> newValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> removedValues = new HashMap<>();
    for (String fieldName : map1.keySet()) {
      if (map2.containsKey(fieldName)) {
        SimpleOrderedMap<Object> oldPropValues = map1.get(fieldName);
        SimpleOrderedMap<Object> newPropValues = map2.get(fieldName);
        if (!oldPropValues.equals(newPropValues)) {
          changedValues.put(fieldName, Arrays.asList(oldPropValues, newPropValues));
        }
      } else {
        removedValues.put(fieldName, map1.get(fieldName));
      }
    }

    for (String fieldName: map2.keySet()) {
      if (!map1.containsKey(fieldName)) {
        newValues.put(fieldName, map2.get(fieldName));
      }
    }

    Map<String, Object> mapDiff = new HashMap<>();
    mapDiff.put(UPDATED_KEY_STRING, changedValues);
    mapDiff.put(ADDED_KEY_STRING, newValues);
    mapDiff.put(REMOVED_KEY_STRING, removedValues);

    return mapDiff;
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

    Map<String, Object>  mapDiff = new HashMap<>();
    mapDiff.put("old", oldList);
    mapDiff.put("new", newList);
    return mapDiff;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldsToPropertyValues(Map<String, SchemaField> fields) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fields.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(false)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldTypesToPropValues(Map<String, FieldType> fieldTypes) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fieldTypes.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(false)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapDynamicFieldToPropValues(IndexSchema.DynamicField[] dynamicFields) {
    return
        Stream.of(dynamicFields)
          .map(df -> Map.entry(df.getPrototype().getName(), df.getPrototype().getNamedPropertyValues(false)))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected static List<SimpleOrderedMap<Object>> getCopyFieldList(ManagedIndexSchema indexSchema) {
    return indexSchema.getCopyFieldProperties(false, null, null);
  }
}
