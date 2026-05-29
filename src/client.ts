import { Chunk, Effect, Queue } from 'effect'

import { IsOfflineError, ServerAheadError, UnknownError, isRejectedPushError, type RejectedPushError } from './errors.ts'
import type { EventDef, Leader } from './leader.ts'
import {
  ClientEvent,
  ROOT,
  compareSeq,
  isEqualEvent,
  merge,
  nextSeqPair,
  type CompositeSeq,
  type PayloadUpstream,
  type SyncState,
} from './sync-state.ts'

export type DecodedEvent = {
  name: string
  args: unknown
}

export type Reducer<TState> = (state: TState, args: unknown, event: ClientEvent) => TState

export type Client<TState> = {
  push: (
    events: readonly DecodedEvent[],
  ) => Effect.Effect<void, RejectedPushError | ServerAheadError | IsOfflineError | UnknownError>
  pullFromLeader: Effect.Effect<void>
  encode: (events: readonly DecodedEvent[]) => Effect.Effect<readonly ClientEvent[]>
  state: Effect.Effect<TState>
  syncState: Effect.Effect<SyncState>
  eventlog: Effect.Effect<readonly ClientEvent[]>
}

export const makeClient = <TState>({
  leader,
  eventDefs,
  reducers,
  initialState,
  clientId,
  sessionId,
}: {
  leader: Leader
  eventDefs: readonly EventDef[]
  reducers: Record<string, Reducer<TState>>
  initialState: TState
  clientId: string
  sessionId: string
}): Effect.Effect<Client<TState>> =>
  Effect.gen(function* () {
  const eventDefsByName = new Map(eventDefs.map((def) => [def.name, def]))
  const leaderState = yield* leader.syncState
  const leaderPullQueue = yield* leader.pullQueue({ cursor: leaderState.localHead })

  let syncState: SyncState = {
    pending: [],
    upstreamHead: leaderState.localHead,
    localHead: leaderState.localHead,
  }
  let eventlog: ClientEvent[] = []
  let state = initialState

  const isClientEvent = (event: ClientEvent): boolean => eventDefsByName.get(event.name)?.clientOnly === true

  const recomputeState = Effect.sync(() => {
    state = [...eventlog]
      .sort((a, b) => compareSeq(a.seqNum, b.seqNum))
      .reduce<TState>((acc, event) => reducers[event.name]?.(acc, event.args, event) ?? acc, initialState)
  })

  const applyEvents = (newEvents: readonly ClientEvent[], rollbackEvents: readonly ClientEvent[] = []) =>
    Effect.gen(function* () {
      const rollbackSeqs = new Set(rollbackEvents.map((event) => eventKey(event)))
      eventlog = eventlog.filter((event) => rollbackSeqs.has(eventKey(event)) === false)

      const existing = new Set(eventlog.map(eventKey))
      for (const event of newEvents) {
        if (existing.has(eventKey(event)) === false) {
          eventlog.push(event)
          existing.add(eventKey(event))
        }
      }

      yield* recomputeState
    })

  const encode: Client<TState>['encode'] = (events) =>
    Effect.sync(() => {
      let baseSeq: CompositeSeq = syncState.localHead
      return events.map((event) => {
        const eventDef = eventDefsByName.get(event.name)
        if (eventDef === undefined) throw new Error(`Unknown event: ${event.name}`)

        const pair = nextSeqPair({
          seq: baseSeq,
          isClient: eventDef.clientOnly === true,
          rebaseGeneration: baseSeq.rebaseGeneration,
        })
        baseSeq = pair.seqNum

        return new ClientEvent({
          name: event.name,
          args: event.args,
          clientId,
          sessionId,
          ...pair,
        })
      })
    })

  const pullFromLeader = Effect.gen(function* () {
    const queuedPayloads = yield* Queue.takeAll(leaderPullQueue)

    for (const { payload } of Chunk.toReadonlyArray(queuedPayloads)) {
      const mergeResult = yield* merge({
        syncState,
        payload,
        isClientEvent,
        eventsEqual: isEqualEvent,
      })

      if (mergeResult._tag === 'reject') {
        return yield* Effect.dieMessage('Client sessions should not reject upstream leader payloads')
      }

      syncState = mergeResult.newSyncState
      if (mergeResult._tag === 'rebase') {
        yield* applyEvents(mergeResult.newEvents, mergeResult.rollbackEvents)
      } else {
        yield* applyEvents(mergeResult.newEvents)
      }
    }
  })

  const push: Client<TState>['push'] = (events) =>
    Effect.gen(function* () {
      const encoded = yield* encode(events)
      const localMerge = yield* merge({
        syncState,
        payload: { _tag: 'local-push', newEvents: encoded },
        isClientEvent,
        eventsEqual: isEqualEvent,
      })

      if (localMerge._tag !== 'advance') {
        return yield* Effect.dieMessage('Expected local client push to advance')
      }

      syncState = localMerge.newSyncState
      yield* applyEvents(localMerge.newEvents)

      const pushed = yield* leader.push(localMerge.newEvents).pipe(Effect.either)
      if (pushed._tag === 'Left') {
        if (isRejectedPushError(pushed.left)) {
          yield* pullFromLeader
          if (syncState.pending.length > 0) {
            yield* leader.push(syncState.pending)
          }
        } else {
          return yield* Effect.fail(pushed.left)
        }
      }

      yield* pullFromLeader
    })

  return {
    push,
    pullFromLeader,
    encode,
    state: Effect.sync(() => state),
    syncState: Effect.sync(() => syncState),
    eventlog: Effect.sync(() => eventlog),
  }
})

const eventKey = (event: ClientEvent) => `${event.seqNum.global},${event.seqNum.client},${event.clientId},${event.sessionId}`
