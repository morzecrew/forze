-- forze 0.25.0 · public.durable_run (PostgresDurableRunStore)
CREATE TABLE public.durable_run (
    run_id              text        NOT NULL,
    name                text        NOT NULL,
    status              text        NOT NULL,
    idempotency_key     text,
    coalesce_key        text,
    input               jsonb,
    output              jsonb,
    error               text,
    tenant_id           uuid,
    attempts            integer     NOT NULL DEFAULT 0,
    leased_until        timestamptz,
    available_at        timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    cancel_requested_at timestamptz,
    cancel_refused_at   timestamptz,
    PRIMARY KEY (run_id)
);

-- required: two submits of one key converge on one run (ON CONFLICT (idempotency_key))
CREATE UNIQUE INDEX ON public.durable_run (idempotency_key);

-- required since 0.17.0: enqueue(coalesce_key=...) joins the one unclaimed pending run per key
CREATE UNIQUE INDEX ON public.durable_run (coalesce_key) WHERE status = 'pending' AND attempts = 0;

-- recommended since 0.18.0: the recovery scan (claim_abandoned) filters on status, and on name for some functions
CREATE INDEX ON public.durable_run (status, name, created_at);

-- recommended since 0.18.0: on a shared tagged table a bound scan also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, status, name, created_at);

-- recommended since 0.17.0: where runs are purged, purge_finished filters on status and updated_at
CREATE INDEX ON public.durable_run (status, updated_at);

-- recommended: the admin listing (list_runs) orders newest first and seeks on it
CREATE INDEX ON public.durable_run (created_at DESC, run_id DESC);

-- recommended: on a shared tagged table a bound listing also filters tenant_id
CREATE INDEX ON public.durable_run (tenant_id, created_at DESC, run_id DESC);

-- forze 0.25.0 · public.durable_step (PostgresDurableFunctionStepAdapter)
CREATE TABLE public.durable_step (
    run_id     text        NOT NULL,
    step_id    text        NOT NULL,
    result     jsonb       NOT NULL,
    tenant_id  uuid,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, step_id)
);

-- forze 0.25.0 · public.durable_schedule (PostgresDurableScheduleStore)
CREATE TABLE public.durable_schedule (
    schedule_id  text        NOT NULL,
    name         text        NOT NULL,
    cron         text        NOT NULL,
    tz           text,
    input        jsonb,
    next_fire_at timestamptz NOT NULL,
    enabled      boolean     NOT NULL DEFAULT true,
    tenant_id    uuid,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (schedule_id)
);

-- recommended: claim_due scans enabled schedules due now, oldest first
CREATE INDEX ON public.durable_schedule (next_fire_at) WHERE enabled;

-- recommended: on a shared tagged table a bound scheduler also filters tenant_id
CREATE INDEX ON public.durable_schedule (tenant_id, next_fire_at) WHERE enabled;

-- forze 0.25.0 · public.outbox_consumed (PostgresOutboxStore)
CREATE TABLE public.outbox_consumed (
    id             uuid        NOT NULL,
    outbox_route   text        NOT NULL,
    event_id       uuid        NOT NULL,
    event_type     text        NOT NULL,
    tenant_id      uuid,
    execution_id   uuid,
    correlation_id uuid,
    causation_id   uuid,
    occurred_at    timestamptz NOT NULL,
    payload        jsonb       NOT NULL,
    status         text        NOT NULL,
    created_at     timestamptz NOT NULL,
    processing_at  timestamptz,
    last_error     text,
    attempts       integer     NOT NULL DEFAULT 0,
    available_at   timestamptz,
    ordering_key   text,
    PRIMARY KEY (id)
);

-- required: staging an event twice keeps one row (ON CONFLICT (outbox_route, event_id))
CREATE UNIQUE INDEX ON public.outbox_consumed (outbox_route, event_id);

-- recommended since 0.5.0: the claim filters a route's pending rows and takes the oldest first
CREATE INDEX ON public.outbox_consumed (outbox_route, status, created_at);

-- forze 0.25.0 · public.outbox (PostgresOutboxStore)
CREATE TABLE public.outbox (
    id             uuid        NOT NULL,
    outbox_route   text        NOT NULL,
    event_id       uuid        NOT NULL,
    event_type     text        NOT NULL,
    tenant_id      uuid,
    execution_id   uuid,
    correlation_id uuid,
    causation_id   uuid,
    occurred_at    timestamptz NOT NULL,
    payload        jsonb       NOT NULL,
    status         text        NOT NULL,
    created_at     timestamptz NOT NULL,
    published_at   timestamptz,
    processing_at  timestamptz,
    last_error     text,
    attempts       integer     NOT NULL DEFAULT 0,
    available_at   timestamptz,
    ordering_key   text,
    hlc            bigint,
    traceparent    text,
    PRIMARY KEY (id)
);

-- required: staging an event twice keeps one row (ON CONFLICT (outbox_route, event_id))
CREATE UNIQUE INDEX ON public.outbox (outbox_route, event_id);

-- recommended since 0.25.0: with hlc_ordering the claim orders by (hlc NULLS LAST, created_at, id)
CREATE INDEX ON public.outbox (outbox_route, status, hlc, created_at, id);

-- recommended since 0.25.0: on a shared tagged table a bound causal claim also filters tenant_id
CREATE INDEX ON public.outbox (tenant_id, outbox_route, status, hlc, created_at, id);

-- forze 0.25.0 · public.outbox_delivered (PostgresOutboxStore delivery log)
CREATE TABLE public.outbox_delivered (
    id           uuid        NOT NULL,
    outbox_route text        NOT NULL,
    event_id     uuid        NOT NULL,
    tenant_id    uuid,
    published_at timestamptz NOT NULL
) PARTITION BY RANGE (published_at);

-- the partition for rows no other partition covers; add yours beside it
CREATE TABLE public.outbox_delivered_default PARTITION OF public.outbox_delivered DEFAULT;

-- recommended: a delivery is looked up by its event id
CREATE INDEX ON public.outbox_delivered (event_id);

-- forze 0.25.0 · public.hlc_checkpoint (PostgresHlcCheckpointStore)
CREATE TABLE public.hlc_checkpoint (
    node_key text   NOT NULL,
    hlc      bigint NOT NULL,
    PRIMARY KEY (node_key)
);

-- forze 0.25.0 · public.inbox (PostgresInboxStore)
CREATE TABLE public.inbox (
    inbox_route  text        NOT NULL,
    message_id   text        NOT NULL,
    processed_at timestamptz NOT NULL,
    PRIMARY KEY (inbox_route, message_id)
);

-- forze 0.25.0 · public.idempotency (PostgresIdempotencyStore)
CREATE TABLE public.idempotency (
    op           text        NOT NULL,
    idem_key     text        NOT NULL,
    payload_hash text        NOT NULL,
    status       text        NOT NULL,
    result       bytea,
    expires_at   timestamptz NOT NULL,
    owner        uuid,
    PRIMARY KEY (op, idem_key)
);

-- forze 0.25.0 · public.counters (PostgresCounterAdapter)
CREATE TABLE public.counters (
    tenant_id  text        NOT NULL,
    suffix     text        NOT NULL,
    value      bigint      NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (tenant_id, suffix)
);

-- forze 0.25.0 · public.principal_epochs (PostgresPrincipalEpochStore)
CREATE TABLE public.principal_epochs (
    principal_id uuid   NOT NULL,
    epoch        bigint NOT NULL,
    PRIMARY KEY (principal_id)
);

-- forze 0.25.0 · public.rotating_credentials (PostgresRotatingCredentialStore)
CREATE TABLE public.rotating_credentials (
    tenant_id    text        NOT NULL,
    ref          text        NOT NULL,
    payload      jsonb       NOT NULL,
    expires_at   timestamptz,
    version      bigint      NOT NULL,
    burnt_reason text,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, ref)
);

-- recommended: the admin listing scans a tenant's grants by updated_at
CREATE INDEX ON public.rotating_credentials (tenant_id, updated_at);
