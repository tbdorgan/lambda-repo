package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class CsvFileHandlerLambda {

        private final SecretsManagerClient secretsClient;
        private final SnsClient snsClient;
        private final DynamoDbClient dynamoClient;
        private final S3Client s3Client;

        // Default constructor for AWS Lambda runtime
        public CsvFileHandlerLambda() {
                this(SecretsManagerClient.create(), SnsClient.create(), DynamoDbClient.create(), S3Client.create());
        }

        // Constructor for testing
        public CsvFileHandlerLambda(SecretsManagerClient secretsClient, SnsClient snsClient,
                        DynamoDbClient dynamoClient, S3Client s3Client) {
                this.secretsClient = secretsClient;
                this.snsClient = snsClient;
                this.dynamoClient = dynamoClient;
                this.s3Client = s3Client;
        }

        public void handleRequest(S3Event event, Context context) {
                context.getLogger().log("Received event " + event.toString());

                String snsSecretName = System.getenv("SNS_SECRET_NAME");
                String snsTopicArn = System.getenv("SNS_TOPIC_ARN");
                String dynamoTableName = System.getenv("DDB_TABLE_NAME");

                try {
                        GetSecretValueResponse secretResponse = secretsClient.getSecretValue(
                                        GetSecretValueRequest.builder().secretId(snsSecretName).build());
                        String emailEndpoint = secretResponse.secretString(); // You can use this in SNS if needed
                        context.getLogger().log("Retrieved secret email: " + emailEndpoint);

                        for (S3Event.S3EventNotificationRecord record : event.getRecords()) {
                                String bucket = record.getS3().getBucket().getName();
                                String key = record.getS3().getObject().getKey();
                                context.getLogger().log("Processing S3 object: " + key + " from bucket: " + bucket);

                                // Download object from S3
                                GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key)
                                                .build();

                                try (InputStream inputStream = s3Client.getObject(getObjectRequest);
                                                CSVReader reader = new CSVReaderBuilder(
                                                                new InputStreamReader(inputStream)).build()) {

                                        String[] headers = reader.readNext(); // Read header row
                                        context.getLogger().log("CSV Headers: " + String.join(", ", headers));

                                        String[] nextLine;
                                        while ((nextLine = reader.readNext()) != null) {
                                                Map<String, AttributeValue> item = new HashMap<>();
                                                item.put("employeeId", AttributeValue.builder().s(nextLine[0]).build());
                                                item.put("firstName", AttributeValue.builder().s(nextLine[1]).build());
                                                item.put("middleName", AttributeValue.builder().s(nextLine[2]).build());
                                                item.put("lastName", AttributeValue.builder().s(nextLine[3]).build());
                                                item.put("email", AttributeValue.builder().s(nextLine[4]).build());
                                                item.put("documentName",
                                                                AttributeValue.builder().s(nextLine[5]).build());
                                                item.put("externalStorage",
                                                                AttributeValue.builder().s(nextLine[6]).build());

                                                dynamoClient.putItem(PutItemRequest.builder().tableName(dynamoTableName)
                                                                .item(item).build());

                                                context.getLogger()
                                                                .log("Inserted record into DynamoDB: " + nextLine[0]);
                                        }
                                }

                                // Send SNS notification
                                snsClient.publish(PublishRequest.builder().topicArn(snsTopicArn).subject("CSV Upload")
                                                .message("CSV uploaded: s3://" + bucket + "/" + key).build());
                                context.getLogger().log("Published SNS notification");
                        }

                } catch (Exception e) {
                        context.getLogger().log("Error processing file: " + e.getMessage());
                        e.printStackTrace();
                }
        }
}
