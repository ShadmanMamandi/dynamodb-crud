
import java.io.IOException;
import java.util.*;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import static com.amazonaws.services.kms.model.KeyManagerType.AWS;


public class MoviesCreateTable {

    static String tableName = "Film";

    public static void main(String[] args) throws Exception {

        // createTable();
       // createItems();
        retrieveItem();
      //  deleteItem();
       updateAddNewAttribute();

    }

    private static AmazonDynamoDB getAmazonDynamoDB() {
        AmazonDynamoDB client = new AmazonDynamoDBClient(new BasicAWSCredentials("AWS-Key", ""));
        client.setRegion(Region.getRegion(Regions.US_WEST_2));
        client.setEndpoint("http://127.0.0.1:8000");
        return client;
    }

    public static void createTable() {


        try {
            AmazonDynamoDB client = getAmazonDynamoDB();
            DynamoDB dynamoDB1 = new DynamoDB(client);
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB1.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("age", KeyType.HASH),// Partition
                            // key
                            new KeySchemaElement("title", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("age", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }


            AmazonDynamoDB client = getAmazonDynamoDB();

            DynamoDB dynamoDB2 = new DynamoDB(client);
            Table table = dynamoDB2.getTable(tableName);

            try {
                Item item = new Item().withPrimaryKey("Id", 1).withNumber("age", 30)
                        .withString("title", "shiva");
                Item item1 = new Item().withPrimaryKey("Id", 2).withNumber("age", 40)
                        .withString("title", "hiva");
                Item item2 = new Item().withPrimaryKey("Id", 3).withNumber("age", 50)
                        .withString("title", "nasrin");
                table.putItem(item);
                System.out.println("ok");
                table.putItem(item1);
                System.out.println("ok");
                table.putItem(item2);
                System.out.println("ok");

            } catch (Exception e) {
                System.err.println("Create items failed.");
                System.err.println(e.getMessage());

            }
        }



    /*private static void retrieveItem() {
        AmazonDynamoDB client = getAmazonDynamoDB();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);

        try {

            GetItemSpec getItemSpec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey("age", 35, "title", "moh"));
            Item item = table.getItem(getItemSpec);


            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }
*/

    private static void retrieveItem() {

        AmazonDynamoDB client = getAmazonDynamoDB();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);

        try {

            Item item = table.getItem("Id",1);

            System.out.println(item.toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }

    }


    private static void updateAddNewAttribute() {
        AmazonDynamoDB client = getAmazonDynamoDB();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
      /*  final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);*/
      /*UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(new PrimaryKey("age", 50, "title", "nasrin")).withUpdateExpression("remove info.actors[0]")
                .withConditionExpression("size(info.actors) > :num").withValueMap(new ValueMap().withNumber(":num", 3))
                .withReturnValues(ReturnValue.UPDATED_NEW);*/
                    // Conditional update (we expect this to fail)

        Item item = new Item().withPrimaryKey("Id", 1).withNumber("age", 35)
                .withString("title", "moh ");
        table.putItem(item);
        try {
            System.out.println("Attempting a conditional update...");
            table.putItem(item);
          //  System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Unable to update item: " );
            System.err.println(e.getMessage());
        }
    }

        /*    UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Id", 121)
                    .withUpdateExpression("set #na = :val1").withNameMap(new NameMap().with("#na", "NewAttribute"))
                    .withValueMap(new ValueMap().withString(":val1", "Some value")).withReturnValues(ReturnValue.ALL_NEW);*/
        /*    UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(new PrimaryKey("age", 50, "title", "nasrin"))
                    .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
                    .withValueMap(new ValueMap().withNumber(":r", 5.5).withString(":p", "Everything happens all at once.")
                            .withList(":a", Arrays.asList("Larry", "Moe", "Curly")))
                    .withReturnValues(ReturnValue.UPDATED_NEW);
            try {
                System.out.println("Updating the item...");
                UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
                System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

            }
            catch (Exception e) {
                System.err.println("Unable to update item: " );
                System.err.println(e.getMessage());
            }
        /*    PutItemOutcome outcome = table
                    .putItem(new Item().withPrimaryKey(new PrimaryKey("age", 50, "title", "nasrin")).withMap("info", infoMap));*/
           //UpdateItemOutcome item = table.updateItem(updateItemSpec);

         //   System.out.println("PutItem succeeded:\n" +  outcome.getPutItemResult());

            // Check the response.
       /*     System.out.println("Printing item after adding new attribute...");
            System.out.println(item.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Failed to add new attribute in " + tableName);
            System.err.println(e.getMessage());
        }*/


 /*   private static void updateMultipleAttributes() {

        Table table = dynamoDB.getTable(tableName);

        try {

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Id", 120)
                    .withUpdateExpression("add #a :val1 set #na=:val2")
                    .withNameMap(new NameMap().with("#a", "Authors").with("#na", "NewAttribute"))
                    .withValueMap(
                            new ValueMap().withStringSet(":val1", "Author YY", "Author ZZ").withString(":val2", "someValue"))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after multiple attribute update...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Failed to update multiple attributes in " + tableName);
            System.err.println(e.getMessage());

        }
    }*/

   /* private static void updateExistingAttributeConditionally() {

        Table table = dynamoDB.getTable(tableName);

        try {

            // Specify the desired price (25.00) and also the condition (price =
            // 20.00)

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Id", 120)
                    .withReturnValues(ReturnValue.ALL_NEW).withUpdateExpression("set #p = :val1")
                    .withConditionExpression("#p = :val2").withNameMap(new NameMap().with("#p", "Price"))
                    .withValueMap(new ValueMap().withNumber(":val1", 25).withNumber(":val2", 20));

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            // Check the response.
            System.out.println("Printing item after conditional update to new attribute...");
            System.out.println(outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Error updating item in " + tableName);
            System.err.println(e.getMessage());
        }
    }*/

    private static void deleteItem() {

        AmazonDynamoDB client = getAmazonDynamoDB();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);


            //    DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            //      .withPrimaryKey(new PrimaryKey("age", 30, "title", "shiva"));
         /*   DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey("id", 1)
                    .withConditionExpression("#ip = :val")
                    .withReturnValues(ReturnValue.ALL_OLD);*/


            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("age", 30, "title", "shiva"));


            try {
                System.out.println("Attempting a conditional delete...");
                table.deleteItem(deleteItemSpec);
                System.out.println("DeleteItem succeeded");
            } catch (Exception e) {
                System.err.println("Unable to delete item: " );
                System.err.println(e.getMessage());
            }


        }
    }




          /*  DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withConditionExpression("#ip = :val").withNameMap(new NameMap().with("#ip", "InPublication"))
                    .withValueMap(new ValueMap().withBoolean(":val", false)).withReturnValues(ReturnValue.ALL_OLD);*/


          /*  DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withConditionExpression("year").withNameMap(new NameMap().with("year","InPublication"))/*withPrimaryKey("title", "shiva")
                    .withConditionExpression("#ip = :val").withNameMap(new NameMap().with("#ip", "InPublication"))
                    .withValueMap(new ValueMap().withBoolean(":val", false)).withReturnValues(ReturnValue.ALL_OLD);

            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error deleting item in " + tableName);
           System.err.println(e.getMessage());*/



