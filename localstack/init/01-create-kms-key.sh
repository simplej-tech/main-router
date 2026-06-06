#!/bin/bash
set -euo pipefail

if awslocal kms list-aliases --query 'Aliases[?AliasName==`alias/demo`].AliasName' --output text | grep -q alias/demo; then
    echo "alias/demo already exists, skipping"
    exit 0
fi

KEY_ID=$(awslocal kms create-key \
    --description "kafka-playground demo key" \
    --query 'KeyMetadata.KeyId' \
    --output text)

awslocal kms create-alias \
    --alias-name alias/demo \
    --target-key-id "$KEY_ID"

echo "Created KMS key $KEY_ID with alias alias/demo"
