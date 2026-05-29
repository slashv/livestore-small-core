# livestore-small-core

A small-core exploration of LiveStore.

## Run

```sh
pnpm install
pnpm test
pnpm dev
```

`pnpm dev` runs [examples/todo.ts](/root/livestore-hardcore/examples/todo.ts), a single-file plain TypeScript example with two clients, one leader per store, and one local FIFO backend.

## Layout

- `src/sync-state.ts`: sequence numbers, event shape, and merge/rebase logic.
- `src/fifo-backend.ts`: local append-only FIFO sync backend.
- `src/leader.ts`: reduced leader processor.
- `src/client.ts`: reduced client-session processor.
- `src/adapter.ts`: tiny plain TypeScript store adapter.
- `test/`: focused tests for sync-state, backend, leader, client, and adapter behavior.
- `docs/sync.html`: standalone illustrated guide to the sync process and edge cases.

## Reference

The upstream LiveStore `dev` branch is cloned locally under `reference/livestore/` for comparison while working on this project. That directory is intentionally ignored by git and should not be committed.

The Effect reference repo is cloned locally under `reference/effect/` at tag `effect@3.21.2`, matching the current LiveStore dev dependency.
