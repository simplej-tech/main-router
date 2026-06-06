# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

`main-router` is the **router** — a small Spring Boot app that consumes one input Kafka topic and produces to one of two output topics based on the `destination` header. Nothing else. The downstream consumer, the publisher CLI, and the mock HTTP backend live in sibling repos.

This repo also hosts the shared dev infra (`compose.yaml`: Kafka + LocalStack KMS) that the rest of the ecosystem connects to.

## Build commands

Single-module Gradle project (Groovy DSL). Java 21 toolchain. Version catalog at `gradle/libs.versions.toml`.

```bash
./gradlew build -x test                              # compile + bootJar
./gradlew bootRun                                    # run the router
./gradlew test                                       # (no tests yet)
docker compose up -d                                 # kafka + localstack (shared ecosystem infra)
docker compose down                                  # stop the infra
./gradlew build --refresh-dependencies               # after common-configs republishes
```

## Ecosystem layout

This is one of 4 active repos in the playground stack:

| Repo | Purpose | Connects to |
|---|---|---|
| `main-router` (this) | router: `requests` → `standard-downstream` / `social-express` | kafka, localstack (consumes encrypted bytes, produces encrypted bytes) |
| `downstream-router` | consumes `standard-downstream`; calls bio + match + social with CB + backpressure + virtual-thread async | kafka, localstack, downstream-service (HTTP) |
| `publisher-cli` | one-shot CLI to publish an encrypted message to `requests` | kafka, localstack |
| `downstream-service` | mock HTTP backend (`/process`, `/match`, `/social`) | none |

Plus the unchanged sibling libs:
- `common-configs` → publishes `com.example:common-configs:0.1.0` (encryption serializer/deserializer + KMS infra)
- `downstream-client-playground` → publishes 3 client libs (`bio-query-client`, `match-client`, `social-client`) that `downstream-router` consumes

Sibling deps via mavenLocal. After a change in a sibling lib: `./gradlew publishToMavenLocal` there, then `./gradlew build --refresh-dependencies` here.

## Routing rule

- **Input topic**: `requests`.
- **Output topics**: `standard-downstream`, `social-express`.
- **Header `destination`** (see `MessageHeaders`): value `"express"` → `social-express`; anything else (including missing / `"standard"`) → `standard-downstream`. The default branch is a deliberate choice so misconfigured / unset producers still get routed to the main flow.

End-to-end encryption: the router consumes via the lib's `DecryptingDeserializer` (so `record.value()` is `Result<byte[], Pair<Exception, byte[]>>`) and produces via the lib's `EncryptingSerializer`. Plaintext stays inside the router process while it reads the header. The downstream-router decrypts the same envelope.

## Source layout

- `src/main/java/com/example/router/RouterApplication.java` — Spring Boot entry point, `@EnableKafka`.
- `src/main/java/com/example/router/RouterListener.java` — `@KafkaListener` on `app.topics.requests`. Calls `routerRateLimiter.acquire()`, reads the `destination` header, decides target topic, sends. No transformation of the payload bytes (modulo the lib's encrypt-on-produce step).
- `src/main/java/com/example/router/kafka/MessageHeaders.java` — `DESTINATION` key + `"standard"` / `"express"` values. Inlined per repo by intent; the schema is the wire contract with `publisher-cli` and `downstream-router`, cheap to duplicate vs. introducing a shared types lib.
- `src/main/java/com/example/router/ratelimit/RateLimiterConfig.java` — single named bean `routerRateLimiter`. Profile-gated on `kafka-rate-limit-enabled`: real Guava-backed at `kafka.rate-limit.router` permits/sec when active, `DisabledRateLimiter` no-op otherwise. The wrapper types come from the `common-configs` lib (generic); this config is the caller's choice of how to use them. Built as `new RateLimiterWrapperImpl(RateLimiter.create(rate))` — the lib's wrapper takes a fully-constructed `RateLimiter`, so the choice of Guava factory (stable vs. warm-up) stays here. No composite — the router has just one flow to throttle.

## Compose

`compose.yaml` brings up Kafka (port 9092) + LocalStack KMS (port 4566). The `localstack/init/01-create-kms-key.sh` script creates `alias/demo` so every app's `kafka.encryption.kms.key-arn=alias/demo` works without manual setup.

All four apps default to `localhost:9092` + `localhost:4566` — there is no app-level compose in any other repo. Spin up `docker compose up -d` here, then `bootRun` each app independently.

## When making changes

- **Routing rule changes**: edit `RouterListener.onMessage` and the `MessageHeaders` constants. Topic names live in `application.yml` under `app.topics.*`.
- **Add a third output topic**: add the property, inject it, branch in `RouterListener`. Keep the default branch (no/unknown header → `standard-downstream`) so unknown destinations don't disappear silently.
- **Add encryption changes / KMS endpoint overrides**: see `common-configs` — the lib owns the serializer/deserializer + KMS infra; this repo just consumes the auto-config.
- **Smoke test end-to-end**:
  ```
  docker compose up -d                           # kafka + localstack
  cd ../downstream-service && ./gradlew bootRun  # backend on :8080
  cd ../downstream-router && ./gradlew bootRun   # consumer on standard-downstream
  cd ../main-router && ./gradlew bootRun    # router on requests
  cd ../publisher-cli && ./gradlew bootRun --args="--destination=standard --id=t1 --message=hi"
  ```
  Expect router log `Routed key=t1 destination=standard -> standard-downstream` and downstream-router log `STANDARD-DOWNSTREAM received id=t1 ... bio=ok match=ok social=ok`.
