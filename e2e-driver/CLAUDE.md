# CLAUDE.md

Guidance for the `e2e-driver` subproject of `main-router`.

## What it is

A runnable end-to-end **driver + validator**. It publishes an encrypted scenario to `requests`, then
consumes + decrypts the results topic (and DLT) and validates each published message produced the
expected `DownstreamResult`. Runs **against an already-running stack** — local compose + the apps, or
a deployed/post-deploy environment. Exits non-zero on any failure or timeout, so it drops into CI or
a post-deploy gate. It's the payload-validating successor to `scripts/smoke-test.sh` (which only
grepped logs).

## Run

Bring the stack up first (infra + all four apps), then:

```bash
# local, against main-router's compose infra + running apps
./gradlew :e2e-driver:bootRun

# deployed / custom env — everything is env-configurable
KAFKA_BOOTSTRAP=broker:9092 KMS_ENDPOINT=https://kms... \
E2E_STANDARD_COUNT=5 E2E_EXPRESS_COUNT=5 E2E_AWAIT_SECONDS=60 \
  java -jar e2e-driver/build/libs/e2e-driver-0.1.0.jar
```

Key env vars: `KAFKA_BOOTSTRAP`, `KMS_ENDPOINT`/`KMS_KEY_ID`/`AWS_REGION`,
`REQUESTS_TOPIC`/`RESULTS_TOPIC`/`DLT_TOPIC`, `E2E_STANDARD_COUNT`/`E2E_EXPRESS_COUNT`,
`E2E_AWAIT_SECONDS`, `E2E_OFFSET_RESET` (default `latest`), `E2E_DLT_ENABLED` (default `true`),
`E2E_DLT_COUNT` (default 2), `DOWNSTREAM_CONTROL_URL` (default `http://localhost:8080`). Set
`E2E_DLT_ENABLED=false` in deployed environments without downstream-service's `/control` endpoint.

## How it works

- `E2eRunner` (`ApplicationRunner`): two phases under a unique `runId` prefix (ids never collide with
  prior runs). **Happy path** — publish standard + express, expect `ok` results on the results topic.
  **Dead-letter** (optional, `app.e2e.dlt.enabled`) — POST `downstream-service`'s `/control/fail`,
  publish standard messages, expect them on the DLT topic with `outcome=error`, then POST
  `/control/ok` (in a `finally`, so the mock is always reset). Kept small (`dlt.count`, default 2) so
  bio's circuit breaker (min 5 calls) stays closed and the listener never pauses. Then validates +
  prints a per-id PASS/FAIL report and `System.exit`s with the code.
- `ResultCollector` (`@KafkaListener`): unique consumer group per run (UUID suffix) + `latest`, so a
  run only sees results it produced. Indexes decrypted `DownstreamResult`s by id, split by
  results-topic vs DLT.
- Validation asserts routing + success: `outcome=ok`, standard ids carry bio+match (+social unless
  disabled), express ids carry social only.
- Inlined wire DTOs (`RequestMessage`, `DownstreamResult`, `MessageHeaders`) per repo convention.

## Gotchas

- **Encryption is real** — the target env's KMS (`alias/demo` on LocalStack locally) must be reachable
  and hold the key, or publish/consume fails.
- **Topics must exist** — the driver waits for partition assignment on results + DLT and fails fast if
  they aren't there (nothing auto-creates them).
- **Runs against a live stack** — it does not start the routers/consumers; bring them up first.
