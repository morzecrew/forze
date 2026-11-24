-- forze 0.17.0, transcribed from pages/docs/reference/durable-schema.md at v0.17.0

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

CREATE INDEX ON public.durable_run (status, created_at);

CREATE INDEX ON public.durable_run (tenant_id, status, created_at);

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
