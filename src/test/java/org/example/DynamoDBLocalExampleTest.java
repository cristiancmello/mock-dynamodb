package org.example;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.*;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBLocalExampleTest {
    static AmazonDynamoDB dynamoDB;

    @BeforeAll
    static void beforeAll() {
        dynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement("PersonID", "HASH"));

        dynamoDB.createTable(List.of(
            new AttributeDefinition("PersonID", ScalarAttributeType.N)
        ), "people", keySchema, new ProvisionedThroughput(10L, 10L));
    }

    @Test
    public void whenPeopleTableCreatedThenListedTable() {
        assertThat(dynamoDB.listTables().getTableNames()).contains("people");
    }

    @Test
    public void whenPersonCreatedThenPutPersonOnDB() {
        PutItemRequest request = new PutItemRequest();
        request.setTableName("people");
        request.setReturnValues(ReturnValue.ALL_OLD);
        Map<String, AttributeValue> map = new HashMap<>();
        map.put("PersonID", (new AttributeValue()).withN("1"));

        request.setItem(map);

        dynamoDB.putItem(request);

        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.setTableName("people");
        getItemRequest.setKey(Map.of("PersonID", (new AttributeValue()).withN("1")));

        assertThat(dynamoDB.getItem(getItemRequest).getItem())
            .containsKey("PersonID");
    }
}
