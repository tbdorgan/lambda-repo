package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.opencsv.CSVReader;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import com.opencsv.CSVReaderBuilder;
import java.io.InputStream;

public class CsvFileHandlerLambda {
        private final SecretsManagerClient secretsClient;
        private final SnsClient snsClient;
        private final DynamoDbClient dynamoClient;

        // Constructor for test injection
        public CsvFileHandlerLambda(SecretsManagerClient secretsClient, SnsClient snsClient,
                        DynamoDbClient dynamoClient) {
                this.secretsClient = secretsClient;
                this.snsClient = snsClient;
                this.dynamoClient = dynamoClient;
        }

        // Default constructor for AWS Lambda runtime
        public CsvFileHandlerLambda() {
                this(SecretsManagerClient.create(), SnsClient.create(), DynamoDbClient.create());
        }

        public void handleRequest(S3Event event, Context context) { // Use S3Event here
                context.getLogger().log("Event :" + event.toString());

                // Environment variables injected by Terraform
                String snsSecretName = System.getenv("SNS_SECRET_NAME"); // Secret in Secrets Manager (email address)
                String snsTopicArn = System.getenv("SNS_TOPIC_ARN"); // SNS topic ARN
                String dynamoTableName = System.getenv("DDB_TABLE_NAME"); // DynamoDB table name

                // Get email from Secrets Manager (if you plan to use it)
                GetSecretValueResponse secretResponse = secretsClient
                                .getSecretValue(GetSecretValueRequest.builder().secretId(snsSecretName).build());
                String emailEndpoint = secretResponse.secretString(); // currently unused
                context.getLogger().log("Email :" + emailEndpoint);

                // Get S3 event details (Updated to handle S3Event)
                for (S3Event.S3EventNotificationRecord record : event.getRecords()) {
                        String bucket = record.getS3().getBucket().getName();
                        String key = record.getS3().getObject().getKey();
                        context.getLogger().log("Bucket :" + bucket);

                        // Fetch CSV file from S3
                        InputStream s3ObjectInputStream = null; // Assume this is initialized with the S3 object content
                        // Fetch the S3 file content
                        // s3ObjectInputStream = s3Client.getObject(new GetObjectRequest(bucket, key));

                        // Parse CSV file
                        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(s3ObjectInputStream))
                                        .withSkipLines(1) // Skip header row
                                        .build()) {
                                String[] nextLine;
                                while ((nextLine = reader.readNext()) != null) {
                                        // Assuming the CSV is in this format: employeeid, firstname, middlename,
                                        // lastname, email, document name, external storage
                                        String employeeId = nextLine[0];
                                        String firstName = nextLine[1];
                                        String middleName = nextLine[2];
                                        String lastName = nextLine[3];
                                        String email = nextLine[4];
                                        String documentName = nextLine[5];
                                        String externalStorage = nextLine[6];

                                        // Write data to DynamoDB
                                        Map<String, AttributeValue> item = new HashMap<>();
                                        item.put("employeeId", AttributeValue.builder().s(employeeId).build());
                                        item.put("firstName", AttributeValue.builder().s(firstName).build());
                                        item.put("middleName", AttributeValue.builder().s(middleName).build());
                                        item.put("lastName", AttributeValue.builder().s(lastName).build());
                                        item.put("email", AttributeValue.builder().s(email).build());
                                        item.put("documentName", AttributeValue.builder().s(documentName).build());
                                        item.put("externalStorage",
                                                        AttributeValue.builder().s(externalStorage).build());
                                        item.put("processedAt", AttributeValue.builder()
                                                        .s(String.valueOf(System.currentTimeMillis())).build());

                                        context.getLogger().log("Calling DynamoDB:");
                                        dynamoClient.putItem(PutItemRequest.builder().tableName(dynamoTableName)
                                                        .item(item).build());
                                }
                        } catch (Exception e) {
                                context.getLogger().log("Error processing CSV file: " + e.getMessage());
                        }

                        // Publish to SNS
                        snsClient.publish(PublishRequest.builder().topicArn(snsTopicArn)
                                        .message("CSV uploaded: s3://" + bucket + "/" + key).subject("CSV Upload")
                                        .build());
                        context.getLogger().log("Sent to SNS:");
                }
        }
}
