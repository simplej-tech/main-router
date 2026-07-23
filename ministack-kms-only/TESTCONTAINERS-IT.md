# Auto-starting MiniStack KMS in the CircuitBreaker IT (Testcontainers)

How to make `StandardDownstreamProcessorCircuitBreakerIT` (in the `downstream-router`
repo) stand up its own MiniStack KMS instead of the fixed-key stub — so
`./gradlew integrationTest` is self-contained and CI-safe, with no `docker compose up`
step beforehand.

This is the "Testcontainers" alternative to the point-at-a-running-MiniStack approach.
It's more moving parts, but the test owns its infra end-to-end.

---

## Why the IT doesn't touch KMS today

`StandardDownstreamProcessorCircuitBreakerIT` lists `TestEncryptionConfig.class` in
`@SpringBootTest(classes = …)`. That class registers a fixed-key `DataKeyProvider`, and
because `common-configs`' real provider is `@ConditionalOnMissingBean`
(`EncryptionConfig.java:49`), the stub wins and the AWS KMS path is never wired. The
encryption is real AES-GCM; the data key just never goes near KMS.

To use MiniStack we drop the stub (so the real `KmsClient` + `AwsKmsDataKeyProvider`
wire up) and point KMS at a container the test starts.

---

## Change 1 — add the Testcontainers dependency

`downstream-router` has no Testcontainers dep yet. Its version comes from the Spring Boot
BOM already on the classpath (`api platform(libs.spring.boot.deps)` manages
`org.testcontainers:*`), so no explicit version is needed.

**`gradle/libs.versions.toml`** — add under `[libraries]`:

```toml
testcontainers = { module = "org.testcontainers:testcontainers" }
```

**`build.gradle`** — add to the `dependencies` block (next to the other
`integrationImplementation` lines, ~`:78`):

```groovy
integrationImplementation libs.testcontainers
```

We only need the core module — MiniStack is a plain image driven via `GenericContainer`,
so no `localstack`/`junit-jupiter` Testcontainers modules are required.

## Change 2 — pin the Docker Engine API version (Docker 29 gotcha)

Docker 29's Engine API rejects the version docker-java negotiates by default, which
surfaces as a container-startup failure. Pin it on the `integrationTest` task.

**`build.gradle`** — inside the existing `tasks.register('integrationTest', Test)` block
(`:82`):

```groovy
tasks.register('integrationTest', Test) {
    // …existing config…
    environment 'DOCKER_API_VERSION', '1.43'   // Docker 29 API-negotiation workaround
    // (equivalently: systemProperty 'api.version', '1.43')
}
```

Drop this line if the runner is on an older Docker where negotiation succeeds.

## Change 3 — start the container and wire it into the IT

All edits are in
`downstream-router/src/test/integration/java/com/example/downstream/it/StandardDownstreamProcessorCircuitBreakerIT.java`.

### 3a. Remove the stub from the context

```diff
 @SpringBootTest(
-        classes = {StandardDownstreamApplication.class, TestEncryptionConfig.class,
-                StandardDownstreamProcessorCircuitBreakerIT.SingleThreadAsyncConfig.class},
+        classes = {StandardDownstreamApplication.class,
+                StandardDownstreamProcessorCircuitBreakerIT.SingleThreadAsyncConfig.class},
```

(You can leave `TestEncryptionConfig.java` in the source set — the sibling
`…SocialToggleCircuitBreakerIT` may still use it — just stop importing it here.)

### 3b. Declare the container + seed a key, and hand the SDK credentials

Add these fields and the static bootstrap alongside the existing `WIREMOCK` setup:

```java
private static final GenericContainer<?> MINISTACK =
        new GenericContainer<>(DockerImageName.parse("ministackorg/ministack:latest"))
                .withExposedPorts(4566)
                .waitingFor(Wait.forLogMessage(".*services available.*", 1));

// The KMS key id this test encrypts against. Captured at runtime from the fresh
// container, so it's always valid and never a stale hardcoded GUID.
private static final String KEY_ID;

static {
    // Dummy creds so the AWS SDK's default credential chain can sign to MiniStack KMS.
    // MUST be set before the Spring context (hence KmsClient) is built.
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    System.setProperty("aws.region", "us-east-1");

    MINISTACK.start();
    KEY_ID = seedKmsKey();      // create a key (+ alias/demo), return the resolved GUID

    WIREMOCK.start();
    STUBS = new DownstreamStubs(WIREMOCK, new ObjectMapper());
}

private static String seedKmsKey() {
    try {
        // awslocal ships in the image and is pre-pointed at the local gateway.
        var created = MINISTACK.execInContainer(
                "awslocal", "kms", "create-key",
                "--query", "KeyMetadata.KeyId", "--output", "text");
        String keyId = created.getStdout().trim();
        MINISTACK.execInContainer(
                "awslocal", "kms", "create-alias",
                "--alias-name", "alias/demo", "--target-key-id", keyId);
        return keyId;
    } catch (Exception e) {
        throw new IllegalStateException("Failed to seed MiniStack KMS key", e);
    }
}
```

> **Why key-arn = the captured GUID, not `alias/demo`.** MiniStack embeds the *resolved*
> key GUID in its ciphertext blob, so decrypt works either way — the only call that must
> resolve the identifier is `GenerateDataKey` (encrypt). Alias resolution there has proven
> flaky on some MiniStack builds (notably arm64), while the raw GUID always works. Because
> Testcontainers gives a fresh container per run, capturing the GUID at startup is both
> robust *and* free of the "hardcoded GUID goes stale on restart" trap. We still create
> `alias/demo` so anything that does resolve aliases keeps working.

### 3c. Point KMS at the container

Add to the existing `@DynamicPropertySource downstreamProps(...)` method:

```java
registry.add("kafka.encryption.kms.endpoint",
        () -> "http://" + MINISTACK.getHost() + ":" + MINISTACK.getMappedPort(4566));
registry.add("kafka.encryption.kms.region", () -> "us-east-1");
registry.add("kafka.encryption.kms.key-arn", () -> KEY_ID);
```

The mapped port is random (Testcontainers picks a free host port), which is exactly why
the endpoint must be set dynamically rather than relying on the `:4566` default in
`application.yml`.

### 3d. Stop the container when the class finishes

Fold it into the existing `@AfterAll`:

```java
@AfterAll
static void stopContainers() {
    WIREMOCK.stop();
    MINISTACK.stop();
}
```

(Testcontainers' Ryuk sidecar also reaps it on JVM exit, but an explicit stop is tidier.)

### 3e. Imports

```java
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
```

---

## What does NOT change

- The whole CB / backpressure test body — phases, counts, awaits — is untouched. Only the
  key-provisioning shifts from stub to real KMS.
- The `single-thread-async` profile, `@EmbeddedKafka`, and WireMock stubs stay as-is.
- No `application.yml` change is required; the `@DynamicPropertySource` overrides win.

## Trade-offs vs. point-at-running-MiniStack

| | Testcontainers (this doc) | Point at running MiniStack |
|---|---|---|
| `./gradlew integrationTest` alone | ✅ works | ❌ needs `docker compose up` first |
| CI-safe | ✅ (Docker-in-CI) | ⚠️ needs a MiniStack service step |
| Startup cost per run | ~5–10s to boot MiniStack | none (already up) |
| Extra deps | Testcontainers | none |
| Docker required on the box | yes | yes |

## Gotchas checklist

- [ ] **Credentials before context** — `System.setProperty("aws.*")` must run in the
      static block *before* `MINISTACK.start()`/context load, else the SDK's credential
      chain has nothing to sign with.
- [ ] **Docker 29** — set `DOCKER_API_VERSION=1.43` (Change 2) or startup fails on
      API negotiation.
- [ ] **Captured GUID, not alias** — avoids the arm64 alias-resolution bug (see note in 3b).
- [ ] **Dynamic endpoint** — always read `getMappedPort(4566)`; never assume `:4566`.
- [ ] **Wait strategy** — `Wait.forLogMessage(".*services available.*", 1)` matches
      MiniStack's `Ready — N services available` banner; adjust if the image's log text
      changes, or use `Wait.forHttp("/_localstack/health").forPort(4566)`.
