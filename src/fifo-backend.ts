import { Effect, Queue, Stream } from 'effect'

import { IsOfflineError, ServerAheadError, UnknownError } from './errors.ts'
import type { GlobalEvent, GlobalSeq } from './sync-state.ts'

export type PullCursor = {
  eventSequenceNumber: GlobalSeq
  metadata: unknown
}

export type PullResPageInfo = { _tag: 'NoMore' } | { _tag: 'MoreKnown'; remaining: number } | { _tag: 'MoreUnknown' }

export const pageInfoNoMore: PullResPageInfo = { _tag: 'NoMore' }
export const pageInfoMoreKnown = (remaining: number): PullResPageInfo => ({ _tag: 'MoreKnown', remaining })

export type PullResItem = {
  batch: readonly { eventEncoded: GlobalEvent; metadata: unknown }[]
  pageInfo: PullResPageInfo
}

export type SyncBackend = {
  connect: Effect.Effect<void, IsOfflineError | UnknownError>
  pull: (
    cursor: PullCursor | undefined,
    options?: { live?: boolean },
  ) => Stream.Stream<PullResItem, IsOfflineError | UnknownError>
  push: (batch: readonly GlobalEvent[]) => Effect.Effect<void, IsOfflineError | UnknownError | ServerAheadError>
  ping: Effect.Effect<void, IsOfflineError | UnknownError>
  currentHead: Effect.Effect<GlobalSeq>
  snapshot: Effect.Effect<readonly GlobalEvent[]>
}

export type FifoBackend = SyncBackend & {
  appendExternal: (batch: readonly GlobalEvent[]) => Effect.Effect<void, IsOfflineError | UnknownError | ServerAheadError>
}

export const makeFifoBackend = Effect.fn('makeFifoBackend')(function* ({
  startConnected = true,
}: {
  startConnected?: boolean
} = {}): Generator<never, FifoBackend> {
  let connected = startConnected
  let events: GlobalEvent[] = []
  const liveQueues = new Set<Queue.Queue<PullResItem>>()

  const requireConnected = Effect.try({
    try: () => {
      if (connected === false) throw new IsOfflineError('FIFO backend is offline')
    },
    catch: (error) => (error instanceof IsOfflineError ? error : new UnknownError(error)),
  })

  const currentHead = Effect.sync(() => events.at(-1)?.seqNum ?? 0)

  const broadcast = (item: PullResItem) =>
    Effect.forEach(liveQueues, (queue) => Queue.offer(queue, item), { discard: true })

  const appendBatch = (batch: readonly GlobalEvent[]) =>
    Effect.gen(function* () {
      yield* requireConnected
      if (batch.length === 0) return

      const head = yield* currentHead
      if (batch[0]!.parentSeqNum !== head) {
        return yield* Effect.fail(new ServerAheadError(head, batch[0]!.parentSeqNum))
      }

      for (let i = 1; i < batch.length; i++) {
        if (batch[i]!.parentSeqNum !== batch[i - 1]!.seqNum) {
          return yield* Effect.fail(new ServerAheadError(batch[i - 1]!.seqNum, batch[i]!.parentSeqNum))
        }
      }

      events = [...events, ...batch]
      yield* broadcast({
        batch: batch.map((eventEncoded) => ({ eventEncoded, metadata: undefined })),
        pageInfo: pageInfoNoMore,
      })
    })

  const pull: SyncBackend['pull'] = (cursor, options) =>
    Stream.unwrapScoped(
      Effect.gen(function* () {
        yield* requireConnected

        const after = cursor?.eventSequenceNumber ?? 0
        const existing = events.filter((event) => event.seqNum > after)
        const existingItems: PullResItem[] =
          existing.length === 0
            ? [{ batch: [], pageInfo: pageInfoNoMore }]
            : [
                {
                  batch: existing.map((eventEncoded) => ({ eventEncoded, metadata: undefined })),
                  pageInfo: pageInfoNoMore,
                },
              ]

        const existingStream = Stream.fromIterable(existingItems)
        if (options?.live !== true) return existingStream

        const queue = yield* Queue.unbounded<PullResItem>().pipe(Effect.acquireRelease(Queue.shutdown))
        liveQueues.add(queue)
        yield* Effect.addFinalizer(() => Effect.sync(() => liveQueues.delete(queue)))

        return existingStream.pipe(Stream.concat(Stream.fromQueue(queue)))
      }),
    )

  return {
    connect: requireConnected,
    pull,
    push: appendBatch,
    ping: requireConnected,
    currentHead,
    snapshot: Effect.sync(() => events),
    appendExternal: appendBatch,
  }
})
