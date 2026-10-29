#!/bin/bash
set -e

echo "=== Initializing LocalStack SQS FIFO Queues ==="

REGION="us-east-1"
ACCOUNT_ID="000000000000"

# 1. Create DLQ FIFO Queue
echo "Creating wager-transactions-dlq.fifo..."
awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"true"}' \
  --region "$REGION"

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:wager-transactions-dlq.fifo"

# 2. Create Main Ingestion FIFO Queue with DLQ Redrive Policy (5 max receives)
echo "Creating wager-transactions.fifo..."
awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"true\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" \
  --region "$REGION"

# 3. Create Outbox Integration Events FIFO Queue
echo "Creating wagering-events.fifo..."
awslocal sqs create-queue \
  --queue-name wagering-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"true"}' \
  --region "$REGION"

echo "=== SQS FIFO Queues Created Successfully ==="
awslocal sqs list-queues --region "$REGION"
