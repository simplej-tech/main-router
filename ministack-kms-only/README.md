# ministack-kms-only

The simplest possible local KMS: stock MiniStack with a single mock key. No Kafka,
no DynamoDB, no custom image, no init sidecar — just a compose service and one seed
script mounted into MiniStack's ready.d hook.

## Step 1 — Start MiniStack (Terminal A)

From **this folder** (`main-router/ministack-kms-only/`), on your Mac:

```bash
cd main-router/ministack-kms-only
docker compose up
```

Leave this running — it holds the terminal and streams MiniStack's logs. On startup
MiniStack prints a banner listing ~74 services and then `Ready — 74 services available`.
That's normal; it always loads every service and "KMS-only" just means we only *use*
KMS. Startup is **not** finished at that line.

A few seconds **after** the `Ready` / `Running on http://0.0.0.0:4566` lines, the seed
script runs. Look for these lines (the seed output is prefixed by `[ministack] stdout:`,
not printed on its own):

```
INFO [ministack] Running ready script: /etc/localstack/init/ready.d/create-kms-key.sh
INFO [kms] Created alias alias/demo -> <key-id>
INFO [ministack]   stdout: created mock KMS key <key-id> (alias/demo)
INFO [ministack] Ready script /etc/localstack/init/ready.d/create-kms-key.sh completed successfully
```

Once you see those, KMS is up on `localhost:4566` with a key aliased `alias/demo`.

Don't want to hunt through the log? Verify the key directly instead:

```bash
docker compose exec kms awslocal kms list-aliases   # -> shows alias/demo
```

(Prefer to get your prompt back? Use `docker compose up -d` to run it detached, then
`docker compose logs -f` to watch, and `docker compose down` to stop it.)

## Step 2 — Talk to it (Terminal B)

Open a **second terminal** on your Mac (Terminal A is busy running MiniStack). These
are ordinary `aws` CLI commands run **on your Mac, against the container** — they are
*not* run inside Docker. You need the AWS CLI installed (`brew install awscli`).

First, point the CLI at the local container instead of real AWS. Do this once per
terminal session (or add it to your shell profile):

```bash
export AWS_ENDPOINT_URL=http://localhost:4566        # send calls to MiniStack, not AWS
export AWS_ACCESS_KEY_ID=test                         # MiniStack ignores the values,
export AWS_SECRET_ACCESS_KEY=test                     # but the CLI requires *some* creds
export AWS_DEFAULT_REGION=us-east-1
```

Then use KMS as normal — the `alias/demo` key already exists:

```bash
# Confirm the key is there:
aws kms list-aliases                 # -> shows alias/demo

# Encrypt something (KMS wants base64-encoded plaintext):
aws kms encrypt --key-id alias/demo --plaintext "$(echo -n hello | base64)"
```

The `encrypt` call returns JSON containing a `CiphertextBlob` — proof the local key
works. When you're done, go back to Terminal A and press `Ctrl-C` (or run
`docker compose down`) to stop MiniStack.

## How it works

- `ministackorg/ministack:latest` exposes everything on one gateway port (`4566`);
  we just don't touch anything but KMS.
- `init/create-kms-key.sh` is mounted into `/etc/localstack/init/ready.d/`, which
  MiniStack runs (LocalStack-compatible) once the gateway is ready. It uses the
  bundled `awslocal` wrapper, so no endpoint or credentials need to be passed.

> If `awslocal` isn't present in your image build, swap the two lines in
> `create-kms-key.sh` for `aws --endpoint-url "$AWS_ENDPOINT_URL" kms ...`
> (`AWS_ENDPOINT_URL` is injected into ready.d scripts automatically).
