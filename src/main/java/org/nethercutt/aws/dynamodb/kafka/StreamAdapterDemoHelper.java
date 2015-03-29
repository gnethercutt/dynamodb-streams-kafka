package org.nethercutt.aws.dynamodb.kafka;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;


public class StreamAdapterDemoHelper {
    private static String tableName = "example-table";
    private static String targetTopic = "dynamostream";
    private static String brokerList = "localhost:9092";
    private static String localddbEndpoint = "http://localhost:8000";


    public static String enableStreamForTable(AmazonDynamoDBClient client, StreamViewType viewType, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest()
            .withTableName(tableName);
        DescribeTableResult describeResult = client.describeTable(describeTableRequest);
        if (describeResult.getTable().getStreamSpecification().isStreamEnabled()) {
            //TODO: what if the viewtype doesn't match
            return describeResult.getTable().getLatestStreamId();
        }

        StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(viewType);
        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
            .withTableName(tableName)
            .withStreamSpecification(streamSpecification);

        UpdateTableResult result = client.updateTable(updateTableRequest);
        return result.getTableDescription().getLatestStreamId();
    }

    public static String createTable(AmazonDynamoDBClient client, String tableName) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
            .withReadCapacityUnits(2L).withWriteCapacityUnits(2L);

        StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(keySchema)
            .withProvisionedThroughput(provisionedThroughput)
            .withStreamSpecification(streamSpecification);

        try {
            System.out.println("Creating table " + tableName);
            CreateTableResult result = client.createTable(createTableRequest);
            return result.getTableDescription().getLatestStreamId();
        } catch(ResourceInUseException e) {
            System.out.println("Table already exists.");
            return describeTable(client, tableName).getTable().getLatestStreamId();
        }
    }

    public static DescribeTableResult describeTable(AmazonDynamoDBClient client, String tableName) {
        return client.describeTable(new DescribeTableRequest().withTableName(tableName));
    }

    public static ScanResult scanTable(AmazonDynamoDBClient client, String tableName) {
        return client.scan(new ScanRequest().withTableName(tableName));
    }

    public static void putItem(AmazonDynamoDBClient client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Id", new AttributeValue().withN(id));
        item.put("attribute-1", new AttributeValue().withS(val));

        PutItemRequest putItemRequest = new PutItemRequest()
            .withTableName(tableName)
            .withItem(item);
        client.putItem(putItemRequest);
    }

    public static void putItem(AmazonDynamoDBClient client, String tableName, java.util.Map<String, AttributeValue> items) {
        PutItemRequest putItemRequest = new PutItemRequest()
            .withTableName(tableName)
            .withItem(items);
        client.putItem(putItemRequest);
    }

    public static void updateItem(AmazonDynamoDBClient client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN(id));

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(new AttributeValue().withS(val));
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withAttributeUpdates(attributeUpdates);
        client.updateItem(updateItemRequest);
    }

    public static void deleteItem(AmazonDynamoDBClient client, String tableName, String id) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN(id));

        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
            .withTableName(tableName)
            .withKey(key);
        client.deleteItem(deleteItemRequest);
    }

    public static String setUpTable(AmazonDynamoDBClient dynamoDBClient, String srcTable) {
        String streamId = StreamAdapterDemoHelper.createTable(dynamoDBClient, srcTable);
        awaitTableCreation(dynamoDBClient, srcTable);
        return streamId;
    }

    public static void awaitTableCreation(AmazonDynamoDBClient dynamoDBClient, String tableName) {
        Integer retries = 0;
        Boolean created = false;
        while(!created && retries < 100) {
            DescribeTableResult result = StreamAdapterDemoHelper.describeTable(dynamoDBClient, tableName);
            created = result.getTable().getTableStatus().equals("ACTIVE");
            if (created) {
                System.out.println("Table is active.");
                return;
            } else {
                retries++;
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    // do nothing
                }
            }
        }
        System.out.println("Timeout after table creation. Exiting...");
        cleanupAndExit(dynamoDBClient, tableName, 1);
    }

    public static void performOps(AmazonDynamoDBClient dynamoDBClient, String tableName) {
        System.out.println("performing ops...");
        StreamAdapterDemoHelper.putItem(dynamoDBClient, tableName, "101", "test1");
        StreamAdapterDemoHelper.updateItem(dynamoDBClient, tableName, "101", "test2");
        StreamAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "101");
        StreamAdapterDemoHelper.putItem(dynamoDBClient, tableName, "102", "demo3");
        StreamAdapterDemoHelper.updateItem(dynamoDBClient, tableName, "102", "demo4");
        StreamAdapterDemoHelper.deleteItem(dynamoDBClient, tableName, "102");
        StreamAdapterDemoHelper.putItem(dynamoDBClient, tableName, "103", "demo5");
    }

    public static void cleanupAndExit(AmazonDynamoDBClient dynamoDBClient, String srcTable, Integer returnValue) {
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(srcTable));
        System.exit(returnValue);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack" , "true");
        Options options = new Options();

        options.addOption("c", false, "create DynamoDB (local) table");
        options.addOption("t", false, "run assorted DynamoDB (local) operations");
        options.addOption("d", false, "delete DynamoDB (local) table");
        
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        
        KafkaDynamoStreamAdapter adapter = new KafkaDynamoStreamAdapter(brokerList, tableName, targetTopic);
        adapter.setClientEndpoints(localddbEndpoint);
        
        if (cmd.hasOption("c")) {
            StreamAdapterDemoHelper.setUpTable(adapter.getDynamoDBClient(), tableName);  
        }
        if (cmd.hasOption("t")) {
            StreamAdapterDemoHelper.performOps(adapter.getDynamoDBClient(), tableName);
            Thread.sleep(1000);
        }
        if (cmd.hasOption("d")) {
            StreamAdapterDemoHelper.cleanupAndExit(adapter.getDynamoDBClient(), tableName, 0);
        }
    }
}
