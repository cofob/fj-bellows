package storage

const currentSchemaVersion = 5

const terminalCostKindsSQL = `'direct_compute', 'billed_compute', 'billing_overhead',
        'image_builder', 'snapshot_storage'`

var migrations = []string{
	`CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_ns INTEGER NOT NULL
);

CREATE TABLE storage_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE price_quotes (
    id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL,
    driver TEXT NOT NULL,
    instance_type TEXT NOT NULL,
    currency TEXT NOT NULL,
    per_hour_nanos INTEGER NOT NULL CHECK (per_hour_nanos >= 0),
    per_month_nanos INTEGER NOT NULL CHECK (per_month_nanos >= 0),
    snapshot_gb_month_nanos INTEGER NOT NULL CHECK (snapshot_gb_month_nanos >= 0),
    minimum_charge_nanos INTEGER NOT NULL CHECK (minimum_charge_nanos >= 0),
    billing_quantum_ns INTEGER NOT NULL CHECK (billing_quantum_ns >= 0),
    minimum_duration_ns INTEGER NOT NULL CHECK (minimum_duration_ns >= 0),
    source TEXT NOT NULL,
    observed_at_ns INTEGER NOT NULL
);
CREATE INDEX price_quotes_lookup
    ON price_quotes(provider, instance_type, observed_at_ns DESC);

CREATE TABLE resources (
    id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    driver TEXT NOT NULL,
    tier TEXT NOT NULL,
    external_id TEXT,
    name TEXT NOT NULL DEFAULT '',
    instance_type TEXT NOT NULL,
    ownership_tag TEXT NOT NULL,
    state TEXT NOT NULL,
    price_quote_id INTEGER REFERENCES price_quotes(id),
    provider_created_at_ns INTEGER,
    opened_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    closed_at_ns INTEGER,
    close_reason TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX resources_external_id
    ON resources(provider, external_id) WHERE external_id IS NOT NULL;
CREATE INDEX resources_recovery ON resources(state, provider, tier);

CREATE TABLE generations (
    id INTEGER PRIMARY KEY,
    resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    generation_number INTEGER NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    image_id TEXT NOT NULL DEFAULT '',
    fingerprint TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    started_at_ns INTEGER NOT NULL,
    ready_at_ns INTEGER,
    closed_at_ns INTEGER,
    UNIQUE(resource_id, generation_number)
);
CREATE INDEX generations_recovery ON generations(state, resource_id);

CREATE TABLE jobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    handle TEXT NOT NULL,
    forgejo_job_id INTEGER,
    attempt INTEGER,
    repository_id INTEGER,
    repository_owner_id INTEGER,
    repository TEXT NOT NULL DEFAULT '',
    workflow TEXT NOT NULL DEFAULT '',
    workflow_file TEXT NOT NULL DEFAULT '',
    job_name TEXT NOT NULL DEFAULT '',
    identity_quality TEXT NOT NULL DEFAULT 'unknown',
    tier TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL DEFAULT '',
    driver TEXT NOT NULL DEFAULT '',
    resource_id INTEGER REFERENCES resources(id) ON DELETE SET NULL,
    generation_id INTEGER REFERENCES generations(id) ON DELETE SET NULL,
    status TEXT NOT NULL,
    conclusion TEXT NOT NULL DEFAULT '',
    infrastructure_failure TEXT NOT NULL DEFAULT '',
    queue_measurement_source TEXT NOT NULL DEFAULT '',
    run_measurement_source TEXT NOT NULL DEFAULT '',
    first_seen_at_ns INTEGER NOT NULL,
    queued_at_ns INTEGER,
    dispatched_at_ns INTEGER,
    runner_started_at_ns INTEGER,
    runner_finished_at_ns INTEGER,
    completed_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    UNIQUE(source, handle)
);
CREATE INDEX jobs_history ON jobs(first_seen_at_ns DESC, id DESC);
CREATE INDEX jobs_workflow ON jobs(source, repository_id, workflow_file, job_name);
CREATE INDEX jobs_recovery ON jobs(completed_at_ns, status);

CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    driver TEXT NOT NULL,
    tier TEXT NOT NULL,
    external_id TEXT,
    name TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    source_resource_id INTEGER REFERENCES resources(id) ON DELETE SET NULL,
    state TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    completed_at_ns INTEGER,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX snapshots_external_id
    ON snapshots(provider, external_id) WHERE external_id IS NOT NULL;
CREATE INDEX snapshots_recovery ON snapshots(state, provider, tier, fingerprint);
CREATE UNIQUE INDEX snapshots_one_active
    ON snapshots(provider, tier) WHERE state = 'active';

CREATE TABLE phases (
    id INTEGER PRIMARY KEY,
    resource_id INTEGER NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    generation_id INTEGER REFERENCES generations(id) ON DELETE SET NULL,
    job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    started_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER,
    outcome TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX phases_recovery ON phases(ended_at_ns, resource_id);
CREATE INDEX phases_job ON phases(job_id, kind);

CREATE TABLE mutations (
    operation_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    provider TEXT NOT NULL,
    tier TEXT NOT NULL DEFAULT '',
    resource_id INTEGER REFERENCES resources(id) ON DELETE SET NULL,
    snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE SET NULL,
    state TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    created_at_ns INTEGER NOT NULL,
    completed_at_ns INTEGER
);
CREATE INDEX mutations_recovery ON mutations(state, provider, tier);

CREATE TABLE cost_entries (
    id INTEGER PRIMARY KEY,
    resource_id INTEGER REFERENCES resources(id) ON DELETE SET NULL,
    snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE SET NULL,
    job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
    price_quote_id INTEGER REFERENCES price_quotes(id),
    kind TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT '',
    nanos INTEGER CHECK (nanos IS NULL OR nanos >= 0),
    known INTEGER NOT NULL CHECK (known IN (0, 1)),
    estimated INTEGER NOT NULL CHECK (estimated IN (0, 1)),
    started_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER NOT NULL,
    recorded_at_ns INTEGER NOT NULL,
    CHECK ((known = 0 AND nanos IS NULL) OR (known = 1 AND nanos IS NOT NULL))
);
CREATE INDEX cost_entries_job ON cost_entries(job_id, kind);
CREATE INDEX cost_entries_time ON cost_entries(started_at_ns, ended_at_ns);
`,
	`DELETE FROM cost_entries
WHERE id NOT IN (
    SELECT MIN(id)
    FROM cost_entries
    GROUP BY COALESCE(resource_id, 0), COALESCE(snapshot_id, 0),
        COALESCE(job_id, 0), kind, started_at_ns, ended_at_ns
);

CREATE UNIQUE INDEX cost_entries_idempotency
    ON cost_entries(
        COALESCE(resource_id, 0),
        COALESCE(snapshot_id, 0),
        COALESCE(job_id, 0),
        kind,
        started_at_ns,
        ended_at_ns
    );
`,
	`DROP INDEX cost_entries_idempotency;

DELETE FROM cost_entries
WHERE id NOT IN (
    SELECT MIN(id)
    FROM cost_entries
    GROUP BY COALESCE(resource_id, 0), COALESCE(snapshot_id, 0),
        COALESCE(job_id, 0), kind, started_at_ns,
        CASE WHEN kind IN (` + terminalCostKindsSQL + `) THEN 0 ELSE ended_at_ns END
);

CREATE UNIQUE INDEX cost_entries_idempotency
    ON cost_entries(
        COALESCE(resource_id, 0),
        COALESCE(snapshot_id, 0),
        COALESCE(job_id, 0),
        kind,
        started_at_ns,
        CASE WHEN kind IN (` + terminalCostKindsSQL + `) THEN 0 ELSE ended_at_ns END
    );
`,
	`CREATE INDEX jobs_routing_profile
    ON jobs(source, repository_id, workflow_file, job_name, status, completed_at_ns);

CREATE TABLE routing_decisions (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    route TEXT NOT NULL,
    required_label TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    profile_source TEXT NOT NULL DEFAULT '',
    profile_samples INTEGER NOT NULL DEFAULT 0 CHECK (profile_samples >= 0),
    predicted_p95_ns INTEGER NOT NULL DEFAULT 0 CHECK (predicted_p95_ns >= 0),
    history_from_ns INTEGER,
    history_to_ns INTEGER,
    fallback_tier TEXT NOT NULL,
    selected_tier TEXT NOT NULL DEFAULT '',
    selected_provider TEXT NOT NULL DEFAULT '',
    selected_driver TEXT NOT NULL DEFAULT '',
    selection_reason TEXT NOT NULL DEFAULT '',
    selected_idle INTEGER NOT NULL DEFAULT 0 CHECK (selected_idle IN (0, 1)),
    score_currency TEXT NOT NULL DEFAULT '',
    selected_cost_nanos INTEGER CHECK (selected_cost_nanos IS NULL OR selected_cost_nanos >= 0),
    fallback_cost_nanos INTEGER CHECK (fallback_cost_nanos IS NULL OR fallback_cost_nanos >= 0),
    first_seen_at_ns INTEGER NOT NULL,
    decided_at_ns INTEGER,
    expires_at_ns INTEGER,
    defer_count INTEGER NOT NULL DEFAULT 0 CHECK (defer_count >= 0),
    last_deferral TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL
);
CREATE INDEX routing_decisions_pending
    ON routing_decisions(state, selected_tier, expires_at_ns);
CREATE INDEX routing_decisions_stats
    ON routing_decisions(route, decided_at_ns);

CREATE TABLE routing_candidate_scores (
    decision_id INTEGER NOT NULL REFERENCES routing_decisions(id) ON DELETE CASCADE,
    tier TEXT NOT NULL,
    provider TEXT NOT NULL,
    driver TEXT NOT NULL,
    instance_type TEXT NOT NULL,
    eligible INTEGER NOT NULL CHECK (eligible IN (0, 1)),
    reason TEXT NOT NULL DEFAULT '',
    idle_workers INTEGER NOT NULL CHECK (idle_workers >= 0),
    active_workers INTEGER NOT NULL CHECK (active_workers >= 0),
    pending_workers INTEGER NOT NULL CHECK (pending_workers >= 0),
    max_instances INTEGER NOT NULL CHECK (max_instances >= 0),
    used_idle INTEGER NOT NULL CHECK (used_idle IN (0, 1)),
    profile_source TEXT NOT NULL DEFAULT '',
    profile_samples INTEGER NOT NULL DEFAULT 0 CHECK (profile_samples >= 0),
    predicted_run_ns INTEGER NOT NULL CHECK (predicted_run_ns >= 0),
    provision_p95_ns INTEGER NOT NULL CHECK (provision_p95_ns >= 0),
    reset_p95_ns INTEGER NOT NULL CHECK (reset_p95_ns >= 0),
    idle_instance_id TEXT NOT NULL DEFAULT '',
    idle_created_at_ns INTEGER,
    idle_paid_hour_end_at_ns INTEGER,
    idle_reap_eligible_at_ns INTEGER,
    price_quote_id INTEGER REFERENCES price_quotes(id),
    native_currency TEXT NOT NULL DEFAULT '',
    native_cost_nanos INTEGER CHECK (native_cost_nanos IS NULL OR native_cost_nanos >= 0),
    fx_rate TEXT NOT NULL DEFAULT '',
    score_cost_nanos INTEGER CHECK (score_cost_nanos IS NULL OR score_cost_nanos >= 0),
    rank INTEGER NOT NULL DEFAULT 0 CHECK (rank >= 0),
    selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
    PRIMARY KEY(decision_id, tier)
);
CREATE INDEX routing_candidate_selected
    ON routing_candidate_scores(decision_id, selected);
`,
	`ALTER TABLE routing_decisions ADD COLUMN optimization_queued INTEGER NOT NULL DEFAULT 0 CHECK (optimization_queued IN (0, 1));
ALTER TABLE routing_decisions ADD COLUMN optimization_active INTEGER NOT NULL DEFAULT 0 CHECK (optimization_active IN (0, 1));
ALTER TABLE routing_decisions ADD COLUMN selected_worker_id TEXT NOT NULL DEFAULT '';
ALTER TABLE routing_decisions ADD COLUMN scheduled_start_at_ns INTEGER;
ALTER TABLE routing_decisions ADD COLUMN scheduled_finish_at_ns INTEGER;
ALTER TABLE routing_decisions ADD COLUMN optimization_queue_position INTEGER NOT NULL DEFAULT 0 CHECK (optimization_queue_position >= 0);
ALTER TABLE routing_decisions ADD COLUMN optimization_wait_ns INTEGER NOT NULL DEFAULT 0 CHECK (optimization_wait_ns >= 0);
ALTER TABLE routing_decisions ADD COLUMN optimization_released_at_ns INTEGER;

ALTER TABLE routing_candidate_scores ADD COLUMN optimization_queued INTEGER NOT NULL DEFAULT 0 CHECK (optimization_queued IN (0, 1));
ALTER TABLE routing_candidate_scores ADD COLUMN scheduled_start_at_ns INTEGER;
ALTER TABLE routing_candidate_scores ADD COLUMN scheduled_finish_at_ns INTEGER;
ALTER TABLE routing_candidate_scores ADD COLUMN optimization_queue_position INTEGER NOT NULL DEFAULT 0 CHECK (optimization_queue_position >= 0);
ALTER TABLE routing_candidate_scores ADD COLUMN optimization_wait_ns INTEGER NOT NULL DEFAULT 0 CHECK (optimization_wait_ns >= 0);

CREATE INDEX routing_decisions_optimization
    ON routing_decisions(route, optimization_active, selected_worker_id, scheduled_finish_at_ns);
`,
}
