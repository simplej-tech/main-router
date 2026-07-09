# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this subproject.

## What this is

`social-express-router` is the **express-path consumer**: it consumes the `social-express` topic
(the router's "express" destination), calls the social API, and produces an aggregated
`DownstreamResult` to the shared results topic — the same topic and wire shape `downstream-router`
produces. It's the express-flow counterpart to `downstream-router`, trimmed to a single downstream call.

It is a **Gradle subproject of the `main-router` repo** (alongside `e2e-driver`) — it uses the root's
version catalog and gradle wrapper and has no own `settings.gradle`/wrapper/catalog.

## Build commands

Run from the `main-router` repo root. Java 21 toolchain; version catalog at the root `gradle/libs.versions.toml`.

```bash
./gradlew :social-express-router:build -x test   # compile + bootJar
./gradlew :social-express-router:bootRun          # run the app (consumer on social-express, http :8082)
./gradlew :social-express-router:integrationTest  # EmbeddedKafka + WireMock end-to-end IT
./gradlew build --refresh-dependencies            # after a sibling lib republishes
```

Sibling deps (via mavenLocal):
- `com.example:common-configs:0.1.0` — envelope encryption + Kafka consumer/producer auto-config + `kafkaTransactionManager`
- `com.example:social-client:0.1.0` — `SocialClient` over OpenAPI okhttp-gson

## Ecosystem position

| Repo | Role |
|---|---|
| `main-router` | routes `requests` → `standard-downstream` (default) / `social-express` (`destination=express`) |
| `downstream-router` | consumes `standard-downstream`; bio + match + social; produces `DownstreamResult` |
| `social-express-router` (this) | consumes `social-express`; social only; produces `DownstreamResult` |
| `publisher-cli` | one-shot CLI test producer |
| `downstream-service` | mock HTTP backend (`/social`, …) |

## Architecture

Single Kafka consumer, transactional consume-process-produce:

- `SocialExpressApplication` — Spring Boot entry point (`@EnableKafka`).
- `SocialExpressListener` — `@KafkaListener(id="social-express-router", topics="${app.topics.social-express}")`,
  `@Transactional("kafkaTransactionManager")`. Decrypts + parses each record, calls `SocialProcessor`,
  and publishes the `DownstreamResult` to `${app.topics.results}` **inside the same transaction** — the
  offset commit and result publish are atomic (exactly-once).
- `SocialProcessor` — calls `SocialClient.lookup(...)` synchronously (must be on the transaction-bound
  listener thread) and folds the outcome into a `DownstreamResult`. Social failures are **caught** and
  returned as an `error`-outcome result rather than thrown, so a bad response commits instead of rolling
  the batch back and replaying forever. Swap the catch for a `DefaultErrorHandler`/DLT to retry instead.
- `model.RequestMessage` / `model.DownstreamResult` — inlined wire DTOs (per-repo convention). The
  `DownstreamResult` is field-for-field wire-compatible with `downstream-router`'s so one consumer can
  read results from both producers; this router only sets the social fields (bio/match stay null).

### Encryption

Delegated to `common-configs`. `kafka.encryption.use-encryption=true` wires the decrypting deserializer
(so `record.value()` is `Result<byte[], Pair<Exception, byte[]>>`) and the encrypting serializer, so the
published result is KMS-envelope-encrypted like every other message.

### SocialClient wiring

No client config class here — setting `app.social.base-url` activates the social-client lib's
auto-config (`SocialConfig`), which provides the `ApiClient` + `SocialApi` + a plain `SocialClient` bean.
There is no circuit breaker / rate limiter / backpressure on this path (unlike `downstream-router`); add
them the same way if the express flow needs them.

## Integration test

`src/test/integration/.../SocialExpressRouterIT` is self-contained (EmbeddedKafka + inline WireMock +
`TestEncryptionConfig` fixed-key stub). It publishes an encrypted `RequestMessage` to `social-express`,
stubs `POST /social`, and asserts the decrypted `DownstreamResult` on `downstream-results`. Run with
`./gradlew integrationTest`.

## When making changes

- **Topic names**: `app.topics.social-express` / `app.topics.results` in `application.yml`.
- **Adding CB/backpressure**: mirror `downstream-router`'s `client/` decorator + `backpressure/` wiring.
- **Result shape**: keep it wire-compatible with `downstream-router`'s `DownstreamResult` if a shared
  results consumer relies on one schema.
