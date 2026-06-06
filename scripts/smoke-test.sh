#!/usr/bin/env bash
# End-to-end smoke test:
#   1. Ensure docker compose stack is up (Kafka + LocalStack)
#   2. Start router, left-processor, right-processor in background
#   3. Publish one left and one right message
#   4. Verify the router routed and the processors decrypted
#   5. Tear down the apps (compose stack stays up)
#
# Prereqs: `./gradlew build` already run; LocalStack KMS alias/demo exists.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${LOG_DIR:-/tmp/kp-logs}"
RUN_ID="${RUN_ID:-$(date +%s)}"
START_TIMEOUT="${START_TIMEOUT:-90}"
DECRYPT_TIMEOUT="${DECRYPT_TIMEOUT:-30}"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

ROUTER_JAR="$REPO_ROOT/router/build/libs/router-0.1.0.jar"
LEFT_JAR="$REPO_ROOT/left-processor/build/libs/left-processor-0.1.0.jar"
RIGHT_JAR="$REPO_ROOT/right-processor/build/libs/right-processor-0.1.0.jar"
PUBLISHER_JAR="$REPO_ROOT/publisher-cli/build/libs/publisher-cli-0.1.0.jar"

for jar in "$ROUTER_JAR" "$LEFT_JAR" "$RIGHT_JAR" "$PUBLISHER_JAR"; do
    [ -f "$jar" ] || { echo "Missing jar: $jar"; echo "Run ./gradlew build first."; exit 1; }
done

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/router.log "$LOG_DIR"/left.log "$LOG_DIR"/right.log

ROUTER_PID=""
LEFT_PID=""
RIGHT_PID=""

cleanup() {
    local exit_code=$?
    echo
    echo "Stopping apps..."
    [ -n "$ROUTER_PID" ] && kill "$ROUTER_PID" 2>/dev/null || true
    [ -n "$LEFT_PID" ]   && kill "$LEFT_PID"   2>/dev/null || true
    [ -n "$RIGHT_PID" ]  && kill "$RIGHT_PID"  2>/dev/null || true
    wait 2>/dev/null || true
    exit $exit_code
}
trap cleanup EXIT INT TERM

wait_for() {
    # wait_for <log-file> <pattern> <timeout-seconds> <description>
    local file=$1 pattern=$2 timeout=$3 desc=$4
    local elapsed=0
    until grep -q -- "$pattern" "$file" 2>/dev/null; do
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "TIMEOUT after ${timeout}s waiting for: $desc"
            echo "--- tail of $file ---"
            tail -n 30 "$file" || true
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

echo "==> Run ID: $RUN_ID"

echo "==> Ensuring docker compose stack is up..."
cd "$REPO_ROOT"
if ! docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -q '^kafka$'; then
    docker compose up -d
fi
# Give Kafka a few seconds to accept connections
sleep 5

echo "==> Starting router..."
java -jar "$ROUTER_JAR" > "$LOG_DIR/router.log" 2>&1 &
ROUTER_PID=$!

echo "==> Starting left-processor..."
java -jar "$LEFT_JAR" > "$LOG_DIR/left.log" 2>&1 &
LEFT_PID=$!

echo "==> Starting right-processor..."
java -jar "$RIGHT_JAR" > "$LOG_DIR/right.log" 2>&1 &
RIGHT_PID=$!

echo "==> Waiting for apps to start (timeout ${START_TIMEOUT}s)..."
wait_for "$LOG_DIR/router.log" "Started RouterApplication" "$START_TIMEOUT" "router startup"
wait_for "$LOG_DIR/left.log"   "Started LeftApplication"   "$START_TIMEOUT" "left startup"
wait_for "$LOG_DIR/right.log"  "Started RightApplication"  "$START_TIMEOUT" "right startup"

LEFT_KEY="left-$RUN_ID"
RIGHT_KEY="right-$RUN_ID"

echo "==> Publishing left message (id=$LEFT_KEY)..."
java -jar "$PUBLISHER_JAR" --destination=left --message="smoke-left-$RUN_ID" --id="$LEFT_KEY" 2>&1 \
    | grep -E 'Published|ERROR' || true

echo "==> Publishing right message (id=$RIGHT_KEY)..."
java -jar "$PUBLISHER_JAR" --destination=right --message="smoke-right-$RUN_ID" --id="$RIGHT_KEY" 2>&1 \
    | grep -E 'Published|ERROR' || true

echo "==> Waiting for processors to decrypt (timeout ${DECRYPT_TIMEOUT}s)..."
wait_for "$LOG_DIR/left.log"  "LEFT received id=$LEFT_KEY"   "$DECRYPT_TIMEOUT" "left decryption"
wait_for "$LOG_DIR/right.log" "RIGHT received id=$RIGHT_KEY" "$DECRYPT_TIMEOUT" "right decryption"

echo
echo "=== ROUTER ==="
grep "Routed key=.*-$RUN_ID" "$LOG_DIR/router.log" || true
echo "=== LEFT ==="
grep "LEFT received id=$LEFT_KEY" "$LOG_DIR/left.log" || true
echo "=== RIGHT ==="
grep "RIGHT received id=$RIGHT_KEY" "$LOG_DIR/right.log" || true
echo
echo "==> Smoke test passed."
