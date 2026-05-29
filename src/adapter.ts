import { Effect } from 'effect'

import { makeFifoBackend, type SyncBackend } from './fifo-backend.ts'
import { makeLeader, type EventDef, type Leader } from './leader.ts'
import { makeClient, type Client, type DecodedEvent, type Reducer } from './client.ts'
import type { SyncState } from './sync-state.ts'

export type Store<TState> = {
  dispatch: (event: DecodedEvent | readonly DecodedEvent[]) => Promise<void>
  pull: () => Promise<void>
  getState: () => Promise<TState>
  getSyncState: () => Promise<SyncState>
  client: Client<TState>
  leader: Leader
  backend: SyncBackend
}

export const createStore = async <TState>({
  eventDefs,
  reducers,
  initialState,
  backend,
  clientId = 'client',
  sessionId = 'session',
}: {
  eventDefs: readonly EventDef[]
  reducers: Record<string, Reducer<TState>>
  initialState: TState
  backend?: SyncBackend
  clientId?: string
  sessionId?: string
}): Promise<Store<TState>> => {
  const program = Effect.gen(function* () {
    const resolvedBackend = backend ?? (yield* makeFifoBackend())
    const leader = yield* makeLeader({ backend: resolvedBackend, eventDefs })
    const client = yield* makeClient({
      leader,
      eventDefs,
      reducers,
      initialState,
      clientId,
      sessionId,
    })

    return { resolvedBackend, leader, client }
  })

  const { resolvedBackend, leader, client } = await Effect.runPromise(program)

  return {
    dispatch: (event) => Effect.runPromise(client.push(Array.isArray(event) ? event : [event])),
    pull: () => Effect.runPromise(client.pullFromLeader),
    getState: () => Effect.runPromise(client.state),
    getSyncState: () => Effect.runPromise(client.syncState),
    client,
    leader,
    backend: resolvedBackend,
  }
}
