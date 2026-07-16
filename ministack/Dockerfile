# Standalone, self-seeding MiniStack image — KMS + DynamoDB only, no Kafka.
#
# Unlike compose.yaml (which runs a separate aws-cli sidecar to seed MiniStack,
# since the MiniStack image ships no `awslocal`), this single image bakes the
# AWS CLI + the init scripts in and seeds itself on startup: it launches
# MiniStack in the background, waits for the gateway, runs ministack/init/*.sh,
# then re-attaches MiniStack as PID 1's foreground process.
#
# Build:  docker build -t ministack-seeded -f ministack/Dockerfile ministack
# Run:    docker run --rm -p 4566:4566 ministack-seeded
# Use:    export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test \
#                AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
#         aws kms list-aliases        # -> alias/demo
#         aws dynamodb list-tables    # -> router-audit-log

FROM ministackorg/ministack:latest

# --- install the AWS CLI (image ships none) -------------------------------
# MiniStack's base is Debian-derived; use the official bundled installer so we
# don't depend on a system python. Swap for `apk add aws-cli` on an Alpine base.
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl unzip ca-certificates bash; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install; \
    rm -rf /tmp/aws /tmp/awscliv2.zip; \
    apt-get purge -y unzip; \
    rm -rf /var/lib/apt/lists/*

# Point every `aws` call at the local MiniStack gateway with dummy creds.
ENV GATEWAY_PORT=4566 \
    LOG_LEVEL=INFO \
    AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION=us-east-1

# The same init scripts compose uses (create alias/demo KMS key + audit table).
COPY init/ /init/
COPY docker-entrypoint.sh /usr/local/bin/seed-entrypoint.sh
RUN chmod +x /usr/local/bin/seed-entrypoint.sh /init/*.sh

EXPOSE 4566
ENTRYPOINT ["/usr/local/bin/seed-entrypoint.sh"]
