# ministack-kms-only

The simplest possible local KMS: stock MiniStack with a single mock key. No Kafka,
no DynamoDB, no custom image, no init sidecar — just a compose service and one seed
script mounted into MiniStack's ready.d hook.

## Run

```bash
docker compose up        # KMS on :4566, seeded with alias/demo
docker compose down
```

## Use

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1

aws kms list-aliases                 # -> alias/demo
aws kms encrypt --key-id alias/demo --plaintext "$(echo -n hello | base64)"
```

## How it works

- `ministackorg/ministack:latest` exposes everything on one gateway port (`4566`);
  we just don't touch anything but KMS.
- `init/create-kms-key.sh` is mounted into `/etc/localstack/init/ready.d/`, which
  MiniStack runs (LocalStack-compatible) once the gateway is ready. It uses the
  bundled `awslocal` wrapper, so no endpoint or credentials need to be passed.

> If `awslocal` isn't present in your image build, swap the two lines in
> `create-kms-key.sh` for `aws --endpoint-url "$AWS_ENDPOINT_URL" kms ...`
> (`AWS_ENDPOINT_URL` is injected into ready.d scripts automatically).
