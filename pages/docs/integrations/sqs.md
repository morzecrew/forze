# SQS Integration

## Page opening

`forze_sqs` provides Amazon SQS-compatible queue ports for Forze. It supports AWS SQS and compatible endpoints such as LocalStack by wrapping async AWS clients, queue URL resolution, message encoding, batch operations, acknowledgement, and dependency registration behind queue contracts.

| Topic | Details |
|------|---------|
| What it provides | `SQSClient`, optional routed client, queue read/write adapters, lifecycle hooks, SQS config, and queue dependency registration. |
| Supported Forze contracts | `QueueQueryDepKey` and `QueueCommandDepKey`, plus `SQSClientDepKey` for infrastructure access. |
| When to use it | Use this integration when queue workloads should run on AWS SQS or SQS-compatible infrastructure, especially for managed at-least-once delivery and serverless/background workers. |

## Installation

```bash
uv add 'forze[sqs]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `sqs` installs `aioboto3`, `aiobotocore`, `botocore`, and SQS type stubs. |
| Required service | AWS SQS or an SQS-compatible endpoint. |
| Local development dependency | LocalStack is the usual local SQS-compatible service. Integration tests normally use testcontainers. |

## Minimal setup

### Client

```python
from forze_sqs import SQSClient

sqs = SQSClient()
```

Use `RoutedSQSClient` when tenant or route identity selects AWS credentials, endpoint, or region.

### Config

```python
from datetime import timedelta
from forze_sqs import SQSConfig

sqs_config = SQSConfig(
    region_name="us-east-1",
    connect_timeout=timedelta(seconds=5),
    read_timeout=timedelta(seconds=20),
    max_pool_connections=50,
    tcp_keepalive=True,
)
```

Queue-level config uses `SQSQueueConfig` with `namespace` and optional `tenant_aware`.

### Deps module

```python
from forze.application.execution import DepsPlan
from forze_sqs import SQSDepsModule

queue_config = {"namespace": "orders", "tenant_aware": True}

sqs_module = SQSDepsModule(
    client=sqs,
    queue_readers={"orders": queue_config},
    queue_writers={"orders": queue_config},
)

deps_plan = DepsPlan.from_modules(sqs_module)
```

The route key should match your `QueueSpec.name`.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_sqs import sqs_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    sqs_lifecycle_step(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=sqs_config,
    )
)
```

Use `routed_sqs_lifecycle_step(client=routed_sqs)` with `RoutedSQSClient` and do not combine routed and non-routed lifecycle steps for the same client.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Queue reads | `ConfigurableSQSQueueRead` / SQS queue adapter. | `QueueQueryDepKey`, route usually equal to `QueueSpec.name`. | SQS is at-least-once; consumers must ack/delete after successful processing and handle duplicates. |
| Queue writes | `ConfigurableSQSQueueWrite` / SQS queue adapter. | `QueueCommandDepKey`, route usually equal to `QueueSpec.name`. | FIFO ordering requires `.fifo` queues and a message group key. Standard queues do not guarantee order. |
| Raw client | `SQSClient` or `RoutedSQSClient`. | `SQSClientDepKey`. | Prefer queue contracts in usecases unless AWS-specific APIs are required. |

## Complete recipe link

See [Background Workflow](../recipes/background-workflow.md) for the long-form background-processing recipe pattern. Use this page for SQS-specific adapter and operations reference.

## Configuration reference

### Connection settings

`sqs_lifecycle_step` requires `endpoint`, `region_name`, `access_key_id`, and `secret_access_key`. Use the AWS endpoint for production and an endpoint such as `http://localhost:4566` for LocalStack.

### Pool settings

`SQSConfig` accepts botocore-compatible options including `max_pool_connections`, `tcp_keepalive`, proxy settings, dualstack/FIPS endpoint toggles, and client certificate options.

### Serialization settings

The adapter encodes message bodies safely for SQS and stores message metadata in SQS message attributes. Use Pydantic models in `QueueSpec` to validate payloads after receive.

### Retry/timeout behavior

`SQSConfig` controls `connect_timeout` and `read_timeout`. `receive(timeout=...)` enables long polling up to the SQS service limit. Visibility timeout, redrive policy, and DLQ settings are queue attributes managed outside Forze.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | Create queues, FIFO queues, redrive policies, visibility timeouts, and IAM permissions outside Forze with AWS console, Terraform, CloudFormation, or LocalStack setup scripts. |
| Cleanup/shutdown | Register `sqs_lifecycle_step` or `routed_sqs_lifecycle_step` so async AWS sessions close cleanly. |
| Idempotency/caching behavior | Standard queues can deliver duplicates. Use FIFO deduplication where appropriate and make consumers idempotent for all side effects. |
| Production caveats | Monitor approximate queue depth/age, configure DLQs, choose visibility timeout longer than handler work, and avoid assuming exact ordering on standard queues. |

## Troubleshooting

| Common error | Likely cause | Fix |
|--------------|--------------|-----|
| `QueueDoesNotExist` or missing queue URL | Queue name is wrong, namespace changed, region differs, or queue was not created. | Verify queue creation, region, namespace, and whether you passed a queue URL or name. |
| Messages return to the queue after processing | Consumer did not ack/delete before visibility timeout expired. | Ack after success and set visibility timeout longer than processing time. |
| FIFO messages are rejected | Queue name or message parameters do not match FIFO requirements. | Use a `.fifo` queue name and provide a stable `key` for `MessageGroupId`. |
| LocalStack works but AWS fails | Endpoint/region/credentials or IAM permissions differ. | Use the AWS endpoint, correct region, real credentials/role, and grant SQS permissions. |
