package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.opencsv.CSVReader;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import com.opencsv.CSVReaderBuilder;
import java.io.InputStream;

public class CsvFileHandlerLambda {

        private final SecretsManagerClient secretsClient;
        private final SnsClient snsClient;
        private final DynamoDbClient dynamoClient;
        private final S3Client s3Client; // Add this line to declare the s3Client

        // Constructor for test injection
        public CsvFileHandlerLambda(SecretsManagerClient secretsClient, SnsClient snsClient,
                        DynamoDbClient dynamoClient, S3Client s3Client) { // Updated constructor
                this.secretsClient = secretsClient;
                this.snsClient = snsClient;
                this.dynamoClient = dynamoClient;
                this.s3Client = s3Client; // Initialize s3Client
        }

        // Default constructor for AWS Lambda runtime
        public CsvFileHandlerLambda() {
                this(SecretsManagerClient.create(), SnsClient.create(), DynamoDbClient.create(), S3Client.create());
        }

        public void handleRequest(S3Event event, Context context) {
                context.getLogger().log("Event: " + event.toString());

                String snsSecretName = System.getenv("SNS_SECRET_NAME");
                String snsTopicArn = System.getenv("SNS_TOPIC_ARN");
                String dynamoTableName = System.getenv("DDB_TABLE_NAME");

                try {
                        GetSecretValueResponse secretResponse = secretsClient.getSecretValue(
                                        GetSecretValueRequest.builder().secretId(snsSecretName).build());
                        String emailEndpoint = secretResponse.secretString();
                        context.getLogger().log("Email: " + emailEndpoint);

                        for (S3Event.S3EventNotificationRecord record : event.getRecords()) {
                                String bucket = record.getS3().getBucket().getName();
                                String key = record.getS3().getObject().getKey();
                                context.getLogger().log("Bucket: " + bucket);

                                // Fetch file from S3
                                try (S3ObjectInputStream s3ObjectInputStream = s3Client
                                                .getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
                                                .stream()) {

                                        // Read CSV data
                                        try (CSVReader reader = new CSVReaderBuilder(
                                                        new InputStreamReader(s3ObjectInputStream)).build()) {

                                                String[] header = reader.readNext(); // Read and process the header
                                                context.getLogger().log("CSV Header: " + String.join(", ", header));

                                                String[] nextLine;
                                                while ((nextLine = reader.readNext()) != null) {
                                                        // Process each CSV row here
                                                        Map<String, AttributeValue> item = new HashMap<>();
                                                        item.put("employeeId", AttributeValue.builder().s(nextLine[0])
                                                                        .build()); // Assuming first column is
                                                                                   // employeeId
                                                        item.put("firstName", AttributeValue.builder().s(nextLine[1])
                                                                        .build()); // Assuming second column is
                                                                                   // firstName
                                                        item.put("middleName", AttributeValue.builder().s(nextLine[2])
                                                                        .build()); // Assuming third column is
                                                                                   // middleName
                                                        item.put("lastName", AttributeValue.builder().s(nextLine[3])
                                                                        .build()); // Assuming fourth column is lastName
                                                        item.put("email", AttributeValue.builder().s(nextLine[4])
                                                                        .build()); // Assuming fifth column is email
                                                        item.put("documentName", AttributeValue.builder().s(nextLine[5])
                                                                        .build()); // Assuming sixth column is
                                                                                   // documentName
                                                        item.put("externalStorage", AttributeValue.builder()
                                                                        .s(nextLine[6]).build()); // Assuming seventh
                                                                                                  // column is
                                                                                                  // externalStorage

                                                        // Put item to DynamoDB
                                                        dynamoClient.putItem(PutItemRequest.builder()
                                                                        .tableName(dynamoTableName).item(item).build());
                                                }
                                        }

                                } catch (Exception e) {
                                        context.getLogger()
                                                        .log("Error reading or processing CSV file: " + e.getMessage());
                                        e.printStackTrace();
                                }

                                // Send an SNS notification
                                snsClient.publish(PublishRequest.builder().topicArn(snsTopicArn)
                                                .message("CSV uploaded: s3://" + bucket + "/" + key)
                                                .subject("CSV Upload").build());
                                context.getLogger().log("Sent to SNS:");
                        }
                } catch (Exception e) {
                        context.getLogger().log("Error processing CSV file: " + e.getMessage());
                        e.printStackTrace();
                }
        }
}
