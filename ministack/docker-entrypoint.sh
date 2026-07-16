#!/usr/bin/env bash
# Start MiniStack in the background, wait for its gateway, seed it, then bring
# MiniStack back to the foreground so it remains PID-1's blocking process and
# the container's lifecycle tracks it (Ctrl-C / `docker stop` -> MiniStack stops).
set -euo pipefail

# The MiniStack server start command. If the base image already defines an
# entrypoint/CMD, substitute it here (e.g. `ministack start` or `/entrypoint.sh`).
MINISTACK_CMD=(ministack)

echo "==> starting MiniStack in background..."
"${MINISTACK_CMD[@]}" &
MINISTACK_PID=$!

# Forward termination to MiniStack so `docker stop` shuts it down cleanly.
trap 'echo "==> stopping MiniStack..."; kill -TERM "$MINISTACK_PID" 2>/dev/null || true' TERM INT

echo "==> waiting for MiniStack gateway on :${GATEWAY_PORT:-4566}..."
until aws kms list-keys >/dev/null 2>&1; do
    # Bail out early if MiniStack died during boot.
    if ! kill -0 "$MINISTACK_PID" 2>/dev/null; then
        echo "MiniStack exited during startup" >&2
        exit 1
    fi
    sleep 2
done

echo "==> seeding (KMS alias/demo + DynamoDB router-audit-log)..."
for s in /init/*.sh; do
    echo "== running $s =="
    bash "$s"
done
echo "==> seed complete; MiniStack ready on :${GATEWAY_PORT:-4566}"

# Block on MiniStack; container stays up as long as it runs.
wait "$MINISTACK_PID"
