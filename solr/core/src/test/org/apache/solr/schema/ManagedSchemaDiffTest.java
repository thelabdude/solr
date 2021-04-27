package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Assert;

import java.util.*;

import static org.apache.solr.schema.ManagedSchemaDiff.diff;
import static org.apache.solr.schema.ManagedSchemaDiff.mapFieldsToPropertyValues;

public class ManagedSchemaDiffTest extends SolrTestCaseJ4 {

  public void testFieldDiff() {
    Map<String, SchemaField> schema1FieldMap = new HashMap<>();
    StrField strFieldType1 = new StrField();
    strFieldType1.setTypeName("string");
    schema1FieldMap.put("strfield", new SchemaField("strfield", strFieldType1));
    schema1FieldMap.put("boolfield", new SchemaField("boolfield", new BoolField()));

    Map<String, SchemaField> schema2FieldMap = new HashMap<>();
    StrField strFieldType2 = new StrField();
    strFieldType2.setTypeName("strings");
    schema2FieldMap.put("strfield", new SchemaField("strfield", strFieldType2));
    schema2FieldMap.put("intfield", new SchemaField("intfield", new IntPointField()));

    Map<String, Object> diff = diff(mapFieldsToPropertyValues(schema1FieldMap), mapFieldsToPropertyValues(schema2FieldMap));
    Assert.assertTrue(diff.containsKey("updated"));
    Assert.assertTrue(diff.containsKey("added"));
    Assert.assertTrue(diff.containsKey("removed"));

    Map<String, Object> changedFields = getInnerMap(diff, "updated");
    Assert.assertEquals(1, changedFields.size());
    Assert.assertTrue(changedFields.containsKey("strfield"));
    Assert.assertEquals(
        Arrays.asList(Map.of("type", "string"), Map.of("type", "strings")),
        changedFields.get("strfield"));

    Map<String, Object> addedFields = getInnerMap(diff, "added");
    Assert.assertEquals(1, addedFields.size());
    Assert.assertTrue(addedFields.containsKey("intfield"));
    Assert.assertEquals(schema2FieldMap.get("intfield").getNamedPropertyValues(true), addedFields.get("intfield"));

    Map<String, Object> removedFields = getInnerMap(diff, "removed");
    Assert.assertEquals(1, removedFields.size());
    Assert.assertTrue(removedFields.containsKey("boolfield"));
    Assert.assertEquals(schema1FieldMap.get("boolfield").getNamedPropertyValues(true), removedFields.get("boolfield"));
  }

  public void testSimpleOrderedMapListDiff() {
    SimpleOrderedMap<Object> obj1 = new SimpleOrderedMap<>();
    obj1.add("name", "obj1");
    obj1.add("type", "objtype1");

    SimpleOrderedMap<Object> obj2 = new SimpleOrderedMap<>();
    obj2.add("name", "obj2");
    obj2.add("type", "objtype2");

    SimpleOrderedMap<Object> obj3 = new SimpleOrderedMap<>();
    obj3.add("name", "obj3");
    obj3.add("type", "objtype3");

    SimpleOrderedMap<Object> obj4 = new SimpleOrderedMap<>();
    obj4.add("name", "obj4");
    obj4.add("type", "objtype4");

    List<SimpleOrderedMap<Object>> list1 = Arrays.asList(obj1, obj2);
    List<SimpleOrderedMap<Object>> list2 = Arrays.asList(obj1, obj3, obj4);

    Map<String, Object> diff = diff(list1, list2);
    Assert.assertTrue(diff.containsKey("old"));
    Assert.assertTrue(diff.containsKey("new"));
    Assert.assertEquals(Arrays.asList(obj2), diff.get("old"));
    Assert.assertEquals(Arrays.asList(obj3, obj4), diff.get("new"));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getInnerMap(Map<String, Object> map, String key) {
    return (Map<String, Object>) map.get(key);
  }
}
