-- forze 0.24.1, transcribed from the reference pages and the store docstrings at v0.24.1

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
    PRIMARY KEY (run_id),
    UNIQUE (idempotency_key)
);

CREATE UNIQUE INDEX ON public.durable_run (coalesce_key) WHERE status = 'pending' AND attempts = 0;

CREATE INDEX ON public.durable_run (status, name, created_at);

CREATE INDEX ON public.durable_run (tenant_id, status, name, created_at);

CREATE INDEX ON public.durable_run (status, updated_at);

CREATE INDEX ON public.durable_run (created_at DESC, run_id DESC);

CREATE INDEX ON public.durable_run (tenant_id, created_at DESC, run_id DESC);

CREATE TABLE public.durable_step (
    run_id     text        NOT NULL,
    step_id    text        NOT NULL,
    result     jsonb       NOT NULL,
    tenant_id  uuid,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, step_id)
);

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

CREATE INDEX ON public.durable_schedule (next_fire_at) WHERE enabled;

CREATE INDEX ON public.durable_schedule (tenant_id, next_fire_at) WHERE enabled;

-- The store tables, transcribed from pages/docs/reference/outbox-schema.md and the store
-- adapters' docstrings at v0.24.1, as a deployment following them has them.

-- public.outbox: the page's table and claim index, its trace and hlc columns and index,
-- and the docstring's tenant-leading claim index.
CREATE TABLE public.outbox (
    id UUID PRIMARY KEY,
    outbox_route TEXT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    tenant_id UUID,
    execution_id UUID,
    correlation_id UUID,
    causation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    processing_at TIMESTAMPTZ,
    last_error TEXT,
    attempts INT NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ,
    ordering_key TEXT,
    UNIQUE (outbox_route, event_id)
);

CREATE INDEX outbox_claim_idx
    ON public.outbox (outbox_route, status, available_at, created_at);

ALTER TABLE public.outbox ADD COLUMN traceparent TEXT;

ALTER TABLE public.outbox ADD COLUMN hlc BIGINT;

CREATE INDEX outbox_claim_hlc_idx
    ON public.outbox (outbox_route, status, available_at, hlc, created_at, id);

CREATE INDEX ON public.outbox (tenant_id, outbox_route, status, created_at);

-- public.outbox_consumed: the page's table, moved over to consume storage as its migration
-- said at v0.24.1.
CREATE TABLE public.outbox_consumed (
    id UUID PRIMARY KEY,
    outbox_route TEXT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    tenant_id UUID,
    execution_id UUID,
    correlation_id UUID,
    causation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    processing_at TIMESTAMPTZ,
    last_error TEXT,
    attempts INT NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ,
    ordering_key TEXT,
    UNIQUE (outbox_route, event_id)
);

CREATE INDEX outbox_consumed_claim_idx
    ON public.outbox_consumed (outbox_route, status, available_at, created_at);

ALTER TABLE public.outbox_consumed DROP COLUMN processing_at, DROP COLUMN published_at;

CREATE TABLE public.outbox_delivered (
    id UUID NOT NULL,
    outbox_route TEXT NOT NULL,
    event_id UUID NOT NULL,
    tenant_id UUID,
    published_at TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (published_at);

CREATE INDEX outbox_delivered_event_idx ON public.outbox_delivered (event_id);

CREATE TABLE public.outbox_delivered_default PARTITION OF public.outbox_delivered DEFAULT;

CREATE TABLE public.hlc_checkpoint (
    node_key TEXT   NOT NULL,
    hlc      BIGINT NOT NULL,
    PRIMARY KEY (node_key)
);

CREATE TABLE public.inbox (
    inbox_route  text NOT NULL,
    message_id   text NOT NULL,
    processed_at timestamptz NOT NULL,
    PRIMARY KEY (inbox_route, message_id)
);

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

CREATE TABLE public.counters (
    tenant_id  text        NOT NULL,
    suffix     text        NOT NULL,
    value      bigint      NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (tenant_id, suffix)
);

CREATE TABLE public.principal_epochs (
    principal_id uuid   PRIMARY KEY,
    epoch        bigint NOT NULL
);

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

CREATE INDEX ON public.rotating_credentials (tenant_id, updated_at);
