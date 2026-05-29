# Sync Distillation Notes

## Reference Files

- LiveStore sequence numbers: `reference/livestore/packages/@livestore/common/src/schema/EventSequenceNumber/*`
- LiveStore event shape: `reference/livestore/packages/@livestore/common/src/schema/LiveStoreEvent/*`
- LiveStore merge/rebase core: `reference/livestore/packages/@livestore/common/src/sync/syncstate.ts`
- LiveStore client session processor: `reference/livestore/packages/@livestore/common/src/sync/ClientSessionSyncProcessor.ts`
- LiveStore leader processor: `reference/livestore/packages/@livestore/common/src/leader-thread/LeaderSyncProcessor.ts`
- Cloudflare FIFO-like backend push/pull: `reference/livestore/packages/@livestore/sync-cf/src/cf-worker/do/{push,pull}.ts`
- Effect reference source: `reference/effect`, tag `effect@3.21.2`

## Reduced Layers

### `src/sync-state.ts`

This file keeps the core data model and merge algorithm:

- Composite sequence numbers are `{ global, client, rebaseGeneration }`.
- Global events increment `global` and reset `client` to zero.
- Client-only events increment `client` while keeping the current `global`.
- `local-push`, `upstream-advance`, and `upstream-rebase` follow LiveStore's pending/confirmed/rebase behavior.
- Rebased pending events increment `rebaseGeneration`.
- Event equality ignores metadata and compares JSON-canonical args, matching the LiveStore fix for undefined values lost over JSON transport.

### `src/fifo-backend.ts`

This is the single sync provider. It models the important Cloudflare Durable Object semantics:

- The backend is an append-only global log.
- A push succeeds only when the first pushed event's parent equals the backend's current head.
- A stale push fails with `ServerAheadError`.
- Pull returns all events after the cursor.
- Live pull subscribers receive pushed chunks, including the pushing client path used by the leader as confirmation.

### `src/leader.ts`

This is the reduced leader processor:

- Validates pushed batches before admission.
- Maintains a leader push head to reject stale or duplicate session pushes.
- Applies local pushes through `SyncState.merge`.
- Does not push client-only events to the backend.
- Pulls backend events with `ignoreClientEvents: true`, like LiveStore's leader.
- Handles backend `ServerAheadError` by pulling, rebasing pending events, rebuilding the backend push queue, and retrying.
- Broadcasts upstream payloads to client pull queues and seeds late queues from cached payloads.

### `src/client.ts`

This is the reduced client-session processor:

- Encodes decoded events from the current local head.
- Optimistically materializes local pushes.
- Pulls leader payloads and applies the same `SyncState.merge` logic.
- On rebase, rolls back by removing rollback events from the local eventlog, appends rebased events, and recomputes state.
- On leader rejection, pulls leader changes, then retries pending events.

### `src/adapter.ts`

This is the single plain TypeScript adapter. It wires one client, one leader, and one FIFO backend into a small store API:

- `dispatch`
- `pull`
- `getState`
- `getSyncState`

## Intentional Reductions

- No SQLite, workers, React, devtools, HTTP, Cloudflare runtime, schema codegen, or materializer hash tracing.
- Rollback is implemented by eventlog removal plus deterministic recomputation instead of SQLite changesets.
- Backend identity mismatch and reconnect policy are omitted because the single local FIFO backend has stable in-process identity.
- Retry/backoff around offline backend failures is omitted; `ServerAheadError` rebase/retry is implemented because it is core sync behavior.

These reductions remove runtime and platform scaffolding while keeping the event numbering, pending confirmation, divergence, rebase, leader rejection, client-only filtering, and backend append semantics testable.
