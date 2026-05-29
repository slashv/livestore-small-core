import { Chunk, Effect, Fiber, Stream } from 'effect'
import { describe, expect, it } from 'vitest'

import { ServerAheadError } from '../src/errors.ts'
import { makeFifoBackend } from '../src/fifo-backend.ts'
import type { GlobalEvent } from '../src/sync-state.ts'

const event = (seqNum: number, parentSeqNum: number, args = `event-${seqNum}`): GlobalEvent => ({
  name: 'todo',
  args,
  seqNum,
  parentSeqNum,
  clientId: 'client-a',
  sessionId: 'session-a',
})

describe('fifo backend', () => {
  it('pushes and pulls append-only global events after a cursor', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()

        yield* backend.push([event(1, 0), event(2, 1)])
        const items = yield* backend.pull({ eventSequenceNumber: 1, metadata: undefined }).pipe(Stream.runCollect)

        return Array.from(items)
      }),
    )

    expect(result).toHaveLength(1)
    expect(result[0]!.batch.map((item) => item.eventEncoded.seqNum)).toStrictEqual([2])
    expect(result[0]!.pageInfo).toStrictEqual({ _tag: 'NoMore' })
  })

  it('rejects pushes when the first parent is not the current backend head', async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        yield* backend.push([event(1, 0)])
        return yield* backend.push([event(3, 2)]).pipe(Effect.flip)
      }),
    )

    expect(error).toBeInstanceOf(ServerAheadError)
    if (!(error instanceof ServerAheadError)) throw new Error('expected ServerAheadError')
    expect(error.minimumExpectedNum).toBe(1)
    expect(error.providedNum).toBe(2)
  })

  it('delivers live pull chunks to subscribers including the pushing client', async () => {
    const pushed = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()

        const pullFiber = yield* backend
          .pull(undefined, { live: true })
          .pipe(
            Stream.filter((item) => item.batch.length > 0),
            Stream.take(1),
            Stream.runCollect,
            Effect.fork,
          )

        yield* backend.push([event(1, 0)])
        const items = yield* Fiber.join(pullFiber)
        return Chunk.toReadonlyArray(items)
      }),
    )

    expect(pushed).toHaveLength(1)
    expect(pushed[0]!.batch.map((item) => item.eventEncoded.seqNum)).toStrictEqual([1])
  })
})
