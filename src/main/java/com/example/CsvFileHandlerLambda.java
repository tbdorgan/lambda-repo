package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

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

        public void handleRequest(S3Event event, Context context) {
                // Environment variables injected by Terraform
                String snsSecretName = System.getenv("SNS_SECRET_NAME"); // Secret in Secrets Manager (email address)
                String snsTopicArn = System.getenv("SNS_TOPIC_ARN"); // SNS topic ARN
                String dynamoTableName = System.getenv("DDB_TABLE_NAME"); // DynamoDB table name

                // Get email from Secrets Manager (if you plan to use it)
                GetSecretValueResponse secretResponse = secretsClient.getSecretValue(
                                GetSecretValueRequest.builder().secretId(snsSecretName).build());
                String emailEndpoint = secretResponse.secretString(); // currently unused

                // Get S3 event details
                String bucket = event.getRecords().get(0).getS3().getBucket().getName();
                String key = event.getRecords().get(0).getS3().getObject().getKey();

                // Write metadata to DynamoDB
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("filename", AttributeValue.builder().s(key).build());
                item.put("bucket", AttributeValue.builder().s(bucket).build());
                item.put("processedAt", AttributeValue.builder().s(String.valueOf(System.currentTimeMillis())).build());

                dynamoClient.putItem(PutItemRequest.builder()
                                .tableName(dynamoTableName)
                                .item(item)
                                .build());

                // Send SNS notification
                snsClient.publish(PublishRequest.builder()
                                .topicArn(snsTopicArn)
                                .message("CSV uploaded: s3://" + bucket + "/" + key)
                                .subject("CSV Upload")
                                .build());
        }
}
