#!/usr/bin/env bash
# End-to-end smoke test for the whole playground:
#   1. Bring up the compose infra (Kafka + LocalStack KMS/Dynamo)
#   2. Build every app (from mavenLocal sibling libs)
#   3. Pre-create the topics, start the full fleet in the background
#        downstream-service (mock backend) + main-router + downstream-router + social-express-router
#   4. Run the e2e-driver — it publishes an encrypted scenario to `requests` and validates the
#      decrypted results on the results topic. The driver's exit code is this script's exit code.
#   5. Tear the apps down (compose infra is left running for fast reruns).
#
# The driver — not log-grepping — is the source of truth: it correlates each published id with its
# DownstreamResult and asserts routing + success. Override scenario size with E2E_STANDARD_COUNT /
# E2E_EXPRESS_COUNT; timeouts with START_TIMEOUT / E2E_AWAIT_SECONDS.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # main-router
PLAYGROUND="$(cd "$REPO_ROOT/.." && pwd)"                       # kafka-router-playground
COMPOSE="$REPO_ROOT/compose.yaml"
LOG_DIR="${LOG_DIR:-/tmp/kp-logs}"
START_TIMEOUT="${START_TIMEOUT:-120}"

# Dummy creds so the AWS SDK can sign requests to LocalStack KMS.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1

DS_JAR="$PLAYGROUND/downstream-service/build/libs/downstream-service-0.1.0.jar"
ROUTER_JAR="$REPO_ROOT/build/libs/main-router-0.1.0.jar"
STD_JAR="$PLAYGROUND/downstream-router/build/libs/downstream-router-0.1.0.jar"
# e2e-driver + social-express-router are subprojects of this repo (built by the main-router build below).
EXP_JAR="$REPO_ROOT/social-express-router/build/libs/social-express-router-0.1.0.jar"
DRIVER_JAR="$REPO_ROOT/e2e-driver/build/libs/e2e-driver-0.1.0.jar"

TOPICS=(requests standard-downstream social-express downstream-results downstream-dlt)

mkdir -p "$LOG_DIR"
PIDS=()

cleanup() {
    local code=$?
    echo
    echo "==> Stopping apps (compose infra left running; 'docker compose -f $COMPOSE down' to stop it)..."
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    exit $code
}
trap cleanup EXIT INT TERM

wait_for() {  # wait_for <log> <pattern> <timeout> <desc>
    local file=$1 pattern=$2 timeout=$3 desc=$4 elapsed=0
    until grep -q -- "$pattern" "$file" 2>/dev/null; do
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "TIMEOUT after ${timeout}s waiting for: $desc"
            echo "--- tail of $file ---"; tail -n 30 "$file" || true
            return 1
        fi
        sleep 2; elapsed=$((elapsed + 2))
    done
}

start_app() {  # start_app <jar> <log-name> <started-pattern>
    local jar=$1 name=$2 pattern=$3
    [ -f "$jar" ] || { echo "Missing jar: $jar (build failed?)"; exit 1; }
    echo "==> Starting $name..."
    java -jar "$jar" > "$LOG_DIR/$name.log" 2>&1 &
    PIDS+=($!)
    wait_for "$LOG_DIR/$name.log" "$pattern" "$START_TIMEOUT" "$name startup"
}

echo "==> Infra up (Kafka + LocalStack)..."
docker compose -f "$COMPOSE" up -d

echo "==> Waiting for LocalStack KMS to be ready..."
elapsed=0
until curl -s http://localhost:4566/_localstack/health 2>/dev/null | grep -qE '"kms": *"(running|available)"'; do
    [ "$elapsed" -ge 60 ] && { echo "TIMEOUT waiting for LocalStack KMS"; exit 1; }
    sleep 2; elapsed=$((elapsed + 2))
done
sleep 3  # let the init ready.d scripts finish creating alias/demo + the audit table

echo "==> Building apps (downstream-service + downstream-router + this repo's root/e2e-driver/social-express-router)..."
( cd "$PLAYGROUND/downstream-service"      && ./gradlew build -x test -q )
( cd "$PLAYGROUND/downstream-router"       && ./gradlew build -x test -q )
( cd "$REPO_ROOT"                          && ./gradlew build -x test -q )   # builds root + :e2e-driver + :social-express-router

echo "==> Pre-creating topics..."
for t in "${TOPICS[@]}"; do
    docker compose -f "$COMPOSE" exec -T kafka \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null
done

rm -f "$LOG_DIR"/*.log
start_app "$DS_JAR"     downstream-service    "Started DownstreamApplication"
start_app "$ROUTER_JAR" main-router           "Started RouterApplication"
start_app "$STD_JAR"    downstream-router      "Started StandardDownstreamApplication"
start_app "$EXP_JAR"    social-express-router  "Started SocialExpressApplication"

echo "==> Fleet is up. Running e2e-driver..."
[ -f "$DRIVER_JAR" ] || { echo "Missing driver jar: $DRIVER_JAR"; exit 1; }
set +e
java -jar "$DRIVER_JAR"
DRIVER_EXIT=$?
set -e

echo
if [ "$DRIVER_EXIT" -eq 0 ]; then
    echo "==> Smoke test PASSED."
else
    echo "==> Smoke test FAILED (driver exit $DRIVER_EXIT). App logs in $LOG_DIR/."
fi
exit "$DRIVER_EXIT"
