#!/bin/bash
# Runs in the ministack-init sidecar (aws CLI). AWS_ENDPOINT_URL targets MiniStack.
set -euo pipefail

if aws kms list-aliases --query 'Aliases[?AliasName==`alias/demo`].AliasName' --output text | grep -q alias/demo; then
    echo "alias/demo already exists, skipping"
    exit 0
fi

KEY_ID=$(aws kms create-key \
    --description "kafka-playground demo key" \
    --query 'KeyMetadata.KeyId' \
    --output text)

aws kms create-alias \
    --alias-name alias/demo \
    --target-key-id "$KEY_ID"

echo "Created KMS key $KEY_ID with alias alias/demo"
