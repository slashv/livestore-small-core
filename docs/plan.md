# livestore-small-core plan

## Goal

Build a single-package TypeScript project that distills LiveStore's sync behavior into the fewest readable layers without changing the core sync logic:

- `src/sync-state.ts`: event sequence numbers, event shape, and the merge/rebase algorithm.
- `src/leader.ts`: leader-side local push processing, backend pull processing, downstream session pull queues, and backend push retry behavior.
- `src/client.ts`: client-session encoding, local optimistic pushes, upstream merge, rollback/rebase, and re-push.
- `src/fifo-backend.ts`: one local FIFO sync backend with Cloudflare-like append-only global log semantics.
- `src/adapter.ts`: a tiny plain TypeScript store adapter that wires client, leader, and backend together.
- `examples/todo.ts`: one single-file example that can run with `pnpm dev`.

The implementation should stay in one project, not a monorepo.

## Reference Sources

- LiveStore reference: `reference/livestore` on the `dev` branch.
- Effect reference: `reference/effect` checked out at tag `effect@3.21.2`.
- Effect dependency: pin this project to `effect@3.21.2`, matching LiveStore dev.
- Effect setup guidance checked from <https://www.effect.solutions/project-setup>; use strict TypeScript settings and Vitest for tests.

## Sync Semantics To Preserve

- Composite sequence numbers use `{ global, client, rebaseGeneration }`.
- Global events advance `global` and reset `client` to zero.
- Client-only events advance `client` while keeping `global`.
- `SyncState.merge` handles three payloads:
  - `local-push`
  - `upstream-advance`
  - `upstream-rebase`
- Pending events are confirmed when upstream emits equal events.
- Divergent pending events are rolled back and rebased onto the upstream head.
- Rebase increments `rebaseGeneration`.
- Leader local pushes are rejected when the leader push head is already ahead.
- Leader drops stale queued pushes when their rebase generation is older than the current local head generation.
- Leader ignores client-only events when merging backend upstream advances.
- Backend push accepts only batches whose first parent equals the current global head; otherwise it fails with `ServerAheadError`.
- A backend `ServerAheadError` is resolved by pulling the backend's newer events and rebasing pending leader events.

## Test Plan

- Sequence number helpers: `nextPair`, `toString`, `fromString`, comparison.
- Sync-state merge cases adapted from LiveStore's focused tests, rewritten for readability.
- FIFO backend push/pull ordering, live pull delivery, and `ServerAheadError`.
- Two clients sharing one leader: local push confirms through backend.
- Two independent leaders sharing one backend: divergent concurrent writes rebase correctly.
- Client-only events stay local and are not pushed to the backend.

## Progress

- [x] Created public GitHub repo and local git project.
- [x] Cloned LiveStore `dev` as ignored reference repo.
- [x] Cloned Effect `effect@3.21.2` as ignored reference repo.
- [x] Scaffold package and strict TypeScript config.
- [x] Implement reduced sync-state core.
- [x] Implement FIFO backend.
- [x] Implement leader processor.
- [x] Implement client processor and plain TS adapter.
- [x] Add example and tests.
- [x] Run `pnpm test`, `pnpm typecheck`, and `pnpm dev`.
