import { Chunk, Effect, Queue, Stream } from 'effect'

import {
  IsOfflineError,
  LeaderAheadError,
  NonMonotonicBatchError,
  ServerAheadError,
  StaleRebaseGenerationError,
  UnknownError,
  type RejectedPushError,
} from './errors.ts'
import type { PullResItem, SyncBackend } from './fifo-backend.ts'
import {
  ClientEvent,
  ROOT,
  merge,
  payloadFromMergeResult,
  seqFromGlobal,
  seqFromString,
  seqGreaterThan,
  seqGreaterThanOrEqual,
  seqToString,
  type CompositeSeq,
  type PayloadUpstream,
  type SyncState,
} from './sync-state.ts'

export type EventDef = {
  name: string
  clientOnly?: boolean
}

export type Leader = {
  push: (
    batch: readonly ClientEvent[],
    options?: { waitForProcessing?: boolean },
  ) => Effect.Effect<void, RejectedPushError | ServerAheadError | IsOfflineError | UnknownError>
  pull: (args: { cursor: CompositeSeq }) => Stream.Stream<{ payload: PayloadUpstream }>
  pullQueue: (args: { cursor: CompositeSeq }) => Effect.Effect<Queue.Queue<{ payload: PayloadUpstream }>>
  pullFromBackend: Effect.Effect<void, IsOfflineError | UnknownError>
  flushBackendPushes: Effect.Effect<void, ServerAheadError | IsOfflineError | UnknownError>
  syncState: Effect.Effect<SyncState>
  eventlog: Effect.Effect<readonly ClientEvent[]>
}

type PullQueueSet = {
  makeQueue: (cursor: CompositeSeq) => Effect.Effect<Queue.Queue<{ payload: PayloadUpstream }>>
  offer: (item: { payload: PayloadUpstream; leaderHead: CompositeSeq }) => Effect.Effect<void>
}

export const makeLeader = Effect.fn('makeLeader')(function* ({
  backend,
  eventDefs,
  initialSyncState = { pending: [], upstreamHead: ROOT, localHead: ROOT },
  localPushBatchSize = 10,
  backendPushBatchSize = 50,
}: {
  backend: SyncBackend | undefined
  eventDefs: readonly EventDef[]
  initialSyncState?: SyncState
  localPushBatchSize?: number
  backendPushBatchSize?: number
}): Generator<never, Leader> {
  const eventDefsByName = new Map(eventDefs.map((def) => [def.name, def]))
  const pullQueues = makePullQueueSet()
  const mutex = Effect.unsafeMakeSemaphore(1)

  let syncState = initialSyncState
  let pushHead = initialSyncState.localHead
  let events: ClientEvent[] = []
  let backendPushQueue: ClientEvent[] = initialSyncState.pending.filter((event) => isClientEvent(event) === false)

  const isClientEvent = (event: ClientEvent): boolean => eventDefsByName.get(event.name)?.clientOnly === true

  const setSyncState = (next: SyncState) =>
    Effect.sync(() => {
      syncState = next
      pushHead = seqGreaterThan(next.localHead, pushHead) ? next.localHead : pushHead
    })

  const appendMaterialized = (newEvents: readonly ClientEvent[], rollbackEvents: readonly ClientEvent[] = []) =>
    Effect.sync(() => {
      const rolledBack = new Set(rollbackEvents.map((event) => seqToString(event.seqNum)))
      events = events.filter((event) => rolledBack.has(seqToString(event.seqNum)) === false)

      const existing = new Set(events.map((event) => seqToString(event.seqNum)))
      for (const event of newEvents) {
        if (existing.has(seqToString(event.seqNum)) === false) {
          events.push(event)
          existing.add(seqToString(event.seqNum))
        }
      }
    })

  const resetBackendPushQueueFromPending = Effect.sync(() => {
    backendPushQueue = syncState.pending.filter((event) => isClientEvent(event) === false)
  })

  const processLocalBatch = (batch: readonly ClientEvent[]) =>
    Effect.gen(function* () {
      let offset = 0
      while (offset < batch.length) {
        const chunk = batch.slice(offset, offset + localPushBatchSize)
        offset += localPushBatchSize

        const currentRebaseGeneration = syncState.localHead.rebaseGeneration
        const stale = chunk.filter((event) => event.seqNum.rebaseGeneration < currentRebaseGeneration)
        if (stale.length > 0) {
          return yield* Effect.fail(
            new StaleRebaseGenerationError(
              currentRebaseGeneration,
              stale[0]!.seqNum.rebaseGeneration,
              stale[0]!.sessionId,
            ),
          )
        }

        const mergeResult = yield* merge({
          syncState,
          payload: { _tag: 'local-push', newEvents: chunk },
          isClientEvent,
        })

        if (mergeResult._tag === 'reject') {
          return yield* Effect.fail(
            new LeaderAheadError(mergeResult.expectedMinimumId, chunk[0]!.seqNum, chunk[0]!.sessionId),
          )
        }
        if (mergeResult._tag === 'rebase') {
          return yield* Effect.dieMessage('The leader should never rebase due to a local push')
        }

        yield* setSyncState(mergeResult.newSyncState)
        yield* appendMaterialized(mergeResult.newEvents)
        yield* pullQueues.offer({
          payload: payloadFromMergeResult(mergeResult),
          leaderHead: mergeResult.newSyncState.localHead,
        })

        backendPushQueue = [
          ...backendPushQueue,
          ...mergeResult.newEvents.filter((event) => isClientEvent(event) === false),
        ]
      }
    }).pipe(mutex.withPermits(1))

  const pullFromBackend = Effect.gen(function* () {
    if (backend === undefined) return

    const chunks = yield* backend
      .pull({ eventSequenceNumber: syncState.upstreamHead.global, metadata: undefined }, { live: false })
      .pipe(Stream.runCollect)

    for (const item of Chunk.toReadonlyArray(chunks)) {
      yield* processBackendPullItem(item)
    }
  })

  const processBackendPullItem = (item: PullResItem) =>
    Effect.gen(function* () {
      if (item.batch.length === 0) return

      const newEvents = item.batch.map(({ eventEncoded, metadata }) =>
        ClientEvent.fromGlobal(eventEncoded, { syncMetadata: metadata }),
      )

      const mergeResult = yield* merge({
        syncState,
        payload: { _tag: 'upstream-advance', newEvents },
        isClientEvent,
        ignoreClientEvents: true,
      })

      if (mergeResult._tag === 'reject') {
        return yield* Effect.dieMessage('The leader should never reject backend upstream advances')
      }

      yield* setSyncState(mergeResult.newSyncState)
      if (mergeResult._tag === 'rebase') {
        yield* appendMaterialized(mergeResult.newEvents, mergeResult.rollbackEvents)
        yield* resetBackendPushQueueFromPending
      } else {
        yield* appendMaterialized(mergeResult.newEvents)
        yield* resetBackendPushQueueFromPending
      }

      yield* pullQueues.offer({
        payload: payloadFromMergeResult(mergeResult),
        leaderHead: mergeResult.newSyncState.localHead,
      })
    }).pipe(mutex.withPermits(1))

  const flushBackendPushes = Effect.gen(function* () {
    if (backend === undefined) return

    while (backendPushQueue.length > 0) {
      const batch = backendPushQueue.slice(0, backendPushBatchSize)
      backendPushQueue = backendPushQueue.slice(batch.length)

      const result = yield* backend.push(batch.map((event) => event.toGlobal())).pipe(Effect.either)
      if (result._tag === 'Right') {
        yield* pullFromBackend
        continue
      }

      if (result.left instanceof ServerAheadError) {
        backendPushQueue = [...batch, ...backendPushQueue]
        yield* pullFromBackend
        yield* resetBackendPushQueueFromPending
        continue
      }

      return yield* Effect.fail(result.left)
    }
  })

  const push: Leader['push'] = (batch) =>
    Effect.gen(function* () {
      if (batch.length === 0) return
      yield* validatePushBatch(batch, pushHead)
      pushHead = batch.at(-1)!.seqNum
      yield* processLocalBatch(batch)
      yield* flushBackendPushes
    })

  return {
    push,
    pull: ({ cursor }) => Stream.unwrapScoped(pullQueues.makeQueue(cursor).pipe(Effect.map(Stream.fromQueue))),
    pullQueue: ({ cursor }) => pullQueues.makeQueue(cursor),
    pullFromBackend,
    flushBackendPushes,
    syncState: Effect.sync(() => syncState),
    eventlog: Effect.sync(() => events),
  }
})

const validatePushBatch = (batch: readonly ClientEvent[], pushHead: CompositeSeq) =>
  Effect.gen(function* () {
    for (let i = 1; i < batch.length; i++) {
      if (seqGreaterThanOrEqual(batch[i - 1]!.seqNum, batch[i]!.seqNum)) {
        return yield* Effect.fail(
          new NonMonotonicBatchError(batch[i - 1]!.seqNum, batch[i]!.seqNum, i, batch[i]!.sessionId),
        )
      }
    }

    if (seqGreaterThanOrEqual(pushHead, batch[0]!.seqNum)) {
      return yield* Effect.fail(new LeaderAheadError(pushHead, batch[0]!.seqNum, batch[0]!.sessionId))
    }
  })

const makePullQueueSet = (): PullQueueSet => {
  const queues = new Set<Queue.Queue<{ payload: PayloadUpstream }>>()
  const cachedPayloads = new Map<string, PayloadUpstream[]>()

  const makeQueue: PullQueueSet['makeQueue'] = (cursor) =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<{ payload: PayloadUpstream }>()

      const payloadsSinceCursor = Array.from(cachedPayloads.entries())
        .flatMap(([seqNumString, payloads]) => payloads.map((payload) => ({ payload, seqNum: seqFromString(seqNumString) })))
        .filter(({ seqNum }) => seqGreaterThan(seqNum, cursor))
        .sort((a, b) => a.seqNum.global - b.seqNum.global || a.seqNum.client - b.seqNum.client)
        .map(({ payload }) => {
          if (payload._tag === 'upstream-advance') {
            return {
              payload: {
                _tag: 'upstream-advance' as const,
                newEvents: payload.newEvents.filter((event) => seqGreaterThan(event.seqNum, cursor)),
              },
            }
          }
          return { payload }
        })

      yield* Queue.offerAll(queue, payloadsSinceCursor)
      queues.add(queue)
      return queue
    })

  const offer: PullQueueSet['offer'] = ({ payload, leaderHead }) =>
    Effect.gen(function* () {
      const key = seqToString(leaderHead)
      cachedPayloads.set(key, [...(cachedPayloads.get(key) ?? []), payload])

      if (payload._tag === 'upstream-advance' && payload.newEvents.length === 0) return

      for (const queue of queues) {
        yield* Queue.offer(queue, { payload })
      }
    })

  return { makeQueue, offer }
}
