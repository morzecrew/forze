# Background workflow

Use this recipe when a request should start long-running work and return before the work finishes.

## Ingredients

- A durable workflow spec from [durable workflow contracts](../core-package/contracts/durable-workflow.md)
- [Temporal Integration](../integrations/temporal.md) for durable workflows, or a queue integration such as [RabbitMQ](../integrations/rabbitmq.md) or [SQS](../integrations/sqs.md)
- A handler that resolves workflow or queue ports from `ExecutionContext`

## Steps

1. Define the command or message DTO that describes the work.
2. Declare the workflow or queue spec with a logical name.
3. Register the same name in the integration dependency module.
4. Resolve the command port from a handler and enqueue/start work.
5. Expose a query endpoint for status or result retrieval when needed.

## Choosing an integration

| Need | Prefer |
|------|--------|
| Durable orchestration, signals, queries, retries | [Temporal](../integrations/temporal.md) |
| Simple message queue with external workers | [RabbitMQ](../integrations/rabbitmq.md) or [SQS](../integrations/sqs.md) |
| Local tests without external services | [Mock](../integrations/mock.md) |

## Learn more

See [Durable contracts](../core-package/contracts/durable.md), [Contracts and Adapters](../concepts/contracts-adapters.md), and the integration page for the backend you choose. For cron-style or delayed enqueue patterns, see [Scheduled queue jobs](scheduled-queue-jobs.md).
