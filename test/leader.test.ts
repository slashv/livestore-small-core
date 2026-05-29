import { Effect } from 'effect'
import { describe, expect, it } from 'vitest'

import { LeaderAheadError } from '../src/errors.ts'
import { makeFifoBackend } from '../src/fifo-backend.ts'
import { makeLeader } from '../src/leader.ts'
import { ClientEvent, ROOT, nextSeqPair, seqToString, type CompositeSeq } from '../src/sync-state.ts'

const eventDefs = [{ name: 'todo' }, { name: 'ui-selected', clientOnly: true }]

const makeEvent = ({
  name = 'todo',
  args,
  base,
  clientId,
  sessionId,
  isClient = false,
}: {
  name?: string
  args: unknown
  base: CompositeSeq
  clientId: string
  sessionId: string
  isClient?: boolean
}) =>
  new ClientEvent({
    name,
    args,
    clientId,
    sessionId,
    ...nextSeqPair({ seq: base, isClient }),
  })

describe('leader', () => {
  it('pushes to the backend and then confirms its own pending event from upstream', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        const leader = yield* makeLeader({ backend, eventDefs })

        yield* leader.push([
          makeEvent({ args: { id: 'a', text: 'A' }, base: ROOT, clientId: 'client-a', sessionId: 'session-a' }),
        ])

        return {
          syncState: yield* leader.syncState,
          backendEvents: yield* backend.snapshot,
        }
      }),
    )

    expect(result.syncState.pending).toHaveLength(0)
    expect(seqToString(result.syncState.upstreamHead)).toBe('e1')
    expect(result.backendEvents.map((event) => event.seqNum)).toStrictEqual([1])
  })

  it('rebases a second leader after the backend rejects an old parent', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        const leaderA = yield* makeLeader({ backend, eventDefs })
        const leaderB = yield* makeLeader({ backend, eventDefs })

        yield* leaderA.push([
          makeEvent({ args: { id: 'a', text: 'A' }, base: ROOT, clientId: 'client-a', sessionId: 'session-a' }),
        ])

        yield* leaderB.push([
          makeEvent({ args: { id: 'b', text: 'B' }, base: ROOT, clientId: 'client-b', sessionId: 'session-b' }),
        ])

        return {
          leaderBState: yield* leaderB.syncState,
          leaderBEvents: yield* leaderB.eventlog,
          backendEvents: yield* backend.snapshot,
        }
      }),
    )

    expect(result.backendEvents.map((event) => [event.seqNum, event.parentSeqNum, event.args])).toStrictEqual([
      [1, 0, { id: 'a', text: 'A' }],
      [2, 1, { id: 'b', text: 'B' }],
    ])
    expect(result.leaderBState.pending).toHaveLength(0)
    expect(seqToString(result.leaderBState.localHead)).toBe('e2')
    expect(result.leaderBEvents.map((event) => seqToString(event.seqNum))).toStrictEqual(['e1', 'e2r1'])
  })

  it('rejects stale pushes whose first event is at or behind the leader push head', async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        const leader = yield* makeLeader({ backend, eventDefs })
        const event = makeEvent({ args: { id: 'a' }, base: ROOT, clientId: 'client-a', sessionId: 'session-a' })

        yield* leader.push([event])
        return yield* leader.push([event]).pipe(Effect.flip)
      }),
    )

    expect(error).toBeInstanceOf(LeaderAheadError)
  })

  it('keeps client-only events out of the backend', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        const leader = yield* makeLeader({ backend, eventDefs })

        yield* leader.push([
          makeEvent({
            name: 'ui-selected',
            args: { id: 'local' },
            base: ROOT,
            clientId: 'client-a',
            sessionId: 'session-a',
            isClient: true,
          }),
        ])

        return {
          syncState: yield* leader.syncState,
          backendEvents: yield* backend.snapshot,
        }
      }),
    )

    expect(result.backendEvents).toHaveLength(0)
    expect(result.syncState.pending.map((event) => seqToString(event.seqNum))).toStrictEqual(['e0.1'])
  })
})
