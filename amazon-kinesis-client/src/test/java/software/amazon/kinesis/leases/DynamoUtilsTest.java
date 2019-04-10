package software.amazon.kinesis.leases;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;

public class DynamoUtilsTest {

  @Test
  public void testSafeGetString() {
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put(null, null);
    Assert.assertNull(DynamoUtils.safeGetString(dynamoRecord, "#"));

    final HashMap<String, AttributeValue> dynamoRecord2 = new HashMap<String, AttributeValue>();
    dynamoRecord2.put("!-\"%", null);
    Assert.assertNull(DynamoUtils.safeGetString(dynamoRecord2, "!-\"%"));

    final HashMap<String, AttributeValue> dynamoRecord3 = new HashMap<String, AttributeValue>();
    Assert.assertNull(DynamoUtils.safeGetString(dynamoRecord3, ""));

    final HashMap<String, AttributeValue> dynamoRecord4 = new HashMap<String, AttributeValue>();
    AttributeValue attribute = DynamoUtils.createAttributeValue("test");
    dynamoRecord4.put("#", attribute);
    Assert.assertEquals("test", DynamoUtils.safeGetString(dynamoRecord4, "#"));
  }


  @Test
  public void testSafeGetSS() {
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("#", null);
    final ArrayList<String> arrayList = new ArrayList<String>();
    Assert.assertEquals(arrayList, DynamoUtils.safeGetSS(dynamoRecord, "key"));

    final HashMap<String, AttributeValue> dynamoRecord2 = new HashMap<String, AttributeValue>();
    Collection<String> collection = new ArrayList();
    collection.add("test_1");
    collection.add("test_2");
    dynamoRecord2.put("#", DynamoUtils.createAttributeValue(collection));
    Assert.assertEquals(collection, DynamoUtils.safeGetSS(dynamoRecord2, "#"));
  }

  @Test
  public void testSafeGetLong() {
    final HashMap<String, AttributeValue> dynamoRecord = new HashMap<String, AttributeValue>();
    dynamoRecord.put("#", null);
    Assert.assertNull(DynamoUtils.safeGetLong(dynamoRecord, "#"));

    final HashMap<String, AttributeValue> dynamoRecord2 = new HashMap<String, AttributeValue>();
    dynamoRecord2.put("#", DynamoUtils.createAttributeValue(12L));
    Assert.assertEquals(Long.valueOf(12), DynamoUtils.safeGetLong(dynamoRecord2, "#"));
  }
}
