
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;


public class DocumentAPIParallelScan1 {

    // total number of sample items
    static int scanItemCount = 300;

    // number of items each scan request should return
    static int scanItemLimit = 10;

    // number of logical segments for parallel scan
    static int parallelScanThreads = 16;

    // table that will be used for scanning
    static String TableName = "ParallelScanTest";

//    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
//    static DynamoDB dynamoDB = new DynamoDB(client);

    static DynamoDB dynamoDB1;


    public static void main(String[] args) throws Exception {

        AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("AWS-Key", ""));

        dynamoDB.setRegion(Region.getRegion(Regions.US_WEST_2));
        dynamoDB.setEndpoint("http://127.0.0.1:8000");
        dynamoDB1 = new DynamoDB(dynamoDB);


        try {

            // Clean up the table
//            deleteTable(TableName);
//            createTable(TableName, 10L, 5L, "Id", "N");


            // Upload sample data for scan
//            uploadSampleProducts(TableName, scanItemCount);

            // Scan the table using multiple threads
//            deleteItem(297);
//            parallelScan(TableName, scanItemLimit, parallelScanThreads);
            updateAddNewAttribute(200);
            retrieveItem();
        }
        catch (AmazonServiceException ase) {
            System.err.println(ase.getMessage());
        }
    }

    private static void parallelScan(String tableName, int itemLimit, int numberOfThreads) {
        System.out.println(
                "Scanning " + tableName + " using " + numberOfThreads + " threads " + itemLimit + " items at a time");
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        // Divide DynamoDB table into logical segments
        // Create one task for scanning each segment
        // Each thread will be scanning one segment
        int totalSegments = numberOfThreads;
        for (int segment = 0; segment < totalSegments; segment++) {
            // Runnable task that will only scan one segment
            ScanSegmentTask task = new ScanSegmentTask(tableName, itemLimit, totalSegments, segment);

            // Execute the task
            executor.execute(task);
        }

        shutDownExecutorService(executor);
    }

    // Runnable task for scanning a single segment of a DynamoDB table
    private static class ScanSegmentTask implements Runnable {

        // DynamoDB table to scan
        private String tableName;

        // number of items each scan request should return
        private int itemLimit;

        // Total number of segments
        // Equals to total number of threads scanning the table in parallel
        private int totalSegments;

        // Segment that will be scanned with by this task
        private int segment;

        public ScanSegmentTask(String tableName, int itemLimit, int totalSegments, int segment) {
            this.tableName = tableName;
            this.itemLimit = itemLimit;
            this.totalSegments = totalSegments;
            this.segment = segment;
        }

        public void run() {
            System.out.println("Scanning " + tableName + " segment " + segment + " out of " + totalSegments
                    + " segments " + itemLimit + " items at a time...");
            int totalScannedItemCount = 0;

            Table table = dynamoDB1.getTable(tableName);

            try {
                ScanSpec spec = new ScanSpec().withMaxResultSize(itemLimit).withTotalSegments(totalSegments)
                        .withSegment(segment);

                ItemCollection<ScanOutcome> items = table.scan(spec);
                Iterator<Item> iterator = items.iterator();

                Item currentItem = null;
                while (iterator.hasNext()) {
                    totalScannedItemCount++;
                    currentItem = iterator.next();
                    System.out.println(currentItem.toString());
                }

            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
            finally {
                System.out.println("Scanned " + totalScannedItemCount + " items from segment " + segment + " out of "
                        + totalSegments + " of " + tableName);
            }
        }
    }

    private static void uploadSampleProducts(String tableName, int itemCount) {
        System.out.println("Adding " + itemCount + " sample items to " + tableName);
        for (int productIndex = 0; productIndex < itemCount; productIndex++) {
            uploadProduct(tableName, productIndex);
        }
    }

    private static void uploadProduct(String tableName, int productIndex) {

        Table table = dynamoDB1.getTable(tableName);

        try {
//            System.out.println("Processing record #" + productIndex);

            Item item = new Item().withPrimaryKey("Id", productIndex)
                    .withString("Title", "Book " + productIndex + " Title").withString("ISBN", "111-1111111111")
                    .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author1"))).withNumber("Price", 2)
                    .withString("Dimensions", "8.5 x 11.0 x 0.5").withNumber("PageCount", 500)
                    .withBoolean("InPublication", true).withString("ProductCategory", "Book");
            System.out.println("Processing record #" +productIndex+"#"+ item);
            table.putItem(item);

        }
        catch (Exception e) {
            System.err.println("Failed to create item " + productIndex + " in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    private static void updateAddNewAttribute(int num) {
        Table table = dynamoDB1.getTable(TableName);

        try {


            Item item = new Item().withPrimaryKey("Id", num)
                    .withString("Title", "Book " + 300 + " Title").withString("ISBN", "111-1111111111")
                    .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author1"))).withNumber("Price", 50)
                    .withString("Dimensions", "8.5 x 11.0 x 0.5").withNumber("PageCount", 600)
                    .withBoolean("InPublication", true).withString("ProductCategory", "Book2222");
            table.putItem(item);

        }
        catch (Exception e) {
            System.err.println("Failed to create item " + 2000 + " in " + TableName);
            System.err.println(e.getMessage());
        }
    }

    private static void deleteTable(String tableName) {
        try {

            Table table = dynamoDB1.getTable(tableName);
            table.delete();
            System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");
            table.waitForDelete();

        }
        catch (Exception e) {
            System.err.println("Failed to delete table " + tableName);
            e.printStackTrace(System.err);
        }
    }

    private static void deleteItem(int number) {

        Table table = dynamoDB1.getTable(TableName);

        try {

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("Id",number);
//                    .withConditionExpression("#ip = :val").withNameMap(new NameMap().with("#ip", "InPublication"))
//                    .withValueMap(new ValueMap().withBoolean(":val", false)).withReturnValues(ReturnValue.ALL_OLD);


            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");


        }
        catch (Exception e) {
            System.err.println("Error deleting item in " + TableName);
            System.err.println(e.getMessage());
        }
    }

    private static void retrieveItem() {

        Table table = dynamoDB1.getTable(TableName);

        try {

            Item item = table.getItem("Id",200);

            System.out.println(item.toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }

    }

    private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
                                    String partitionKeyName, String partitionKeyType) {
        System.out.println("Attempting to create table; please wait...");

        createTable(tableName, readCapacityUnits, writeCapacityUnits, partitionKeyName, partitionKeyType, null, null);
    }

    private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
                                    String partitionKeyName, String partitionKeyType, String sortKeyName, String sortKeyType) {

        try {
            System.out.println("Creating table " + tableName);

            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
            // key

            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions
                    .add(new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort
                // key
                attributeDefinitions
                        .add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType(sortKeyType));
            }

            Table table = dynamoDB1.createTable(tableName, keySchema, attributeDefinitions, new ProvisionedThroughput()
                    .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));
            System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
            table.waitForActive();

        }
        catch (Exception e) {
            System.err.println("Failed to create table " + tableName);
            e.printStackTrace(System.err);
        }
    }

    private static void shutDownExecutorService(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();

            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
