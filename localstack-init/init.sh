#!/bin/bash
# LocalStack initialization script
# Runs on container startup via /etc/localstack/init/ready.d

set -e

echo "Waiting for LocalStack to be ready..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"kms": "running"'; do
    echo "Waiting for KMS..."
    sleep 2
done

echo "LocalStack ready, initializing resources..."

# Create KMS key for DEK wrapping
aws --endpoint-url=http://localhost:4566 kms create-key \
    --description "KP Proxy DEK wrapping key" \
    --key-usage ENCRYPT_DECRYPT \
    --origin AWS_KMS \
    --region us-east-1 \
    --query 'KeyMetadata.KeyId' --output text > /tmp/kms_key_id.txt

KMS_KEY_ID=$(cat /tmp/kms_key_id.txt)
echo "Created KMS key: $KMS_KEY_ID"

# Create alias
aws --endpoint-url=http://localhost:4566 kms create-alias \
    --alias-name alias/kp-proxy-test \
    --target-key-id $KMS_KEY_ID \
    --region us-east-1

echo "Created KMS alias: alias/kp-proxy-test"

# Create S3 buckets
aws --endpoint-url=http://localhost:4566 s3 mb s3://kafka-spill --region us-east-1
aws --endpoint-url=http://localhost:4566 s3 mb s3://kafka-wal --region us-east-1

echo "Created S3 buckets: kafka-spill, kafka-wal"

# Set bucket policies for public read (for testing)
aws --endpoint-url=http://localhost:4566 s3api put-bucket-policy \
    --bucket kafka-spill \
    --policy '{"Version":"2012-10-17","Statement":[{"Sid":"PublicRead","Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::kafka-spill/*"}]}' \
    --region us-east-1

aws --endpoint-url=http://localhost:4566 s3api put-bucket-policy \
    --bucket kafka-wal \
    --policy '{"Version":"2012-10-17","Statement":[{"Sid":"PublicRead","Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::kafka-wal/*"}]}' \
    --region us-east-1

# Create DynamoDB table for breaker state
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name breaker-state \
    --attribute-definitions \
        AttributeName=region,AttributeType=S \
        AttributeName=channel,AttributeType=S \
    --key-schema \
        AttributeName=region,KeyType=HASH \
        AttributeName=channel,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1

echo "Created DynamoDB table: breaker-state"

# Create DynamoDB table for reconciler state
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name reconciler-state \
    --attribute-definitions \
        AttributeName=topic,AttributeType=S \
        AttributeName=partition,AttributeType=N \
        AttributeName=window_start,AttributeType=N \
    --key-schema \
        AttributeName=topic,KeyType=HASH \
        AttributeName=partition,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1

echo "Created DynamoDB table: reconciler-state"

# Create secrets for testing
aws --endpoint-url=http://localhost:4566 secretsmanager create-secret \
    --name kp-proxy/test/kms-key-id \
    --secret-string "$KMS_KEY_ID" \
    --region us-east-1

echo "Created secret: kp-proxy/test/kms-key-id"

echo "LocalStack initialization complete!"