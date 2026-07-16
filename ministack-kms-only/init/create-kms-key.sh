#!/bin/sh
# ready.d hook: runs once after MiniStack's gateway is up. `awslocal` ships in the
# image and is pre-pointed at the local gateway, so no endpoint/creds needed here.
set -e

KEY_ID=$(awslocal kms create-key --description "mock demo key" \
    --query KeyMetadata.KeyId --output text)
awslocal kms create-alias --alias-name alias/demo --target-key-id "$KEY_ID"

echo "created mock KMS key $KEY_ID (alias/demo)"
