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

public class CsvHandler {
    private final SecretsManagerClient secretsClient = SecretsManagerClient.create();
    private final SnsClient snsClient = SnsClient.create();
    private final DynamoDbClient dynamoClient = DynamoDbClient.create();

    public void handleRequest(S3Event event, Context context) {
        String secretName = System.getenv("SNS_SECRET_NAME");
        String topicArn = System.getenv("SNS_TOPIC_ARN");
        String tableName = System.getenv("DDB_TABLE_NAME");

        GetSecretValueResponse secretResponse = secretsClient.getSecretValue(
                GetSecretValueRequest.builder().secretId(secretName).build());
        String email = secretResponse.secretString(); // assuming plain email string

        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("filename", AttributeValue.builder().s(key).build());
        item.put("bucket", AttributeValue.builder().s(bucket).build());
        item.put("processedAt", AttributeValue.builder().s(String.valueOf(System.currentTimeMillis())).build());

        dynamoClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build());

        snsClient.publish(PublishRequest.builder()
                .topicArn(topicArn)
                .message("CSV uploaded: s3://" + bucket + "/" + key)
                .subject("CSV Upload")
                .build());
    }
}
