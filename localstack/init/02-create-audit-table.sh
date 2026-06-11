#!/bin/bash
set -euo pipefail

TABLE=router-audit-log

if awslocal dynamodb describe-table --table-name "$TABLE" >/dev/null 2>&1; then
    echo "$TABLE already exists, skipping"
    exit 0
fi

awslocal dynamodb create-table \
    --table-name "$TABLE" \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

echo "Created DynamoDB table $TABLE (hash key: id)"
