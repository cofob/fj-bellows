# router

Package `router` owns automatic, cost-aware job-to-tier decisions. It polls
each automatic Forgejo label once, scores ordinary tier capacity, persists the
choice in SQLite, and replays assigned jobs into the selected tier's existing
job source. Cloud providers remain unaware of routing.

Profiles are isolated by Forgejo source, repository, workflow, and job name.
The router prefers the configured fallback until enough normal completion
samples exist, then compares candidate marginal costs using provider quotes,
fixed exchange rates, lifecycle P95s, live idle workers, and virtual capacity
reservations. Assignments and candidate scorecards are durable; unclaimed
leases are renewed only while the job remains visible in a successful queue
poll.

An optional bounded optimization queue schedules jobs behind a reusable
hourly worker while its paid window can cover the predicted job and reset.
The durable worker ID and schedule survive restarts. The tier reconciler owns
the actual provisioning decision and suppresses capacity only for the exact
handles currently marked as optimization-queued.
