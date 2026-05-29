import { Effect, Exit, Cause } from 'effect'
import { describe, expect, it } from 'vitest'

import {
  ClientEvent,
  ROOT,
  compareSeq,
  isEqualEvent,
  makeSeq,
  merge,
  nextSeqPair,
  seqFromString,
  seqToString,
  type ClientEventInit,
  type CompositeSeqInput,
  type SyncState,
} from '../src/sync-state.ts'

class TestEvent extends ClientEvent {
  readonly isClient: boolean

  constructor(input: ClientEventInit & { isClient: boolean }) {
    super(input)
    this.isClient = input.isClient
  }

  static make(seqNum: CompositeSeqInput, parentSeqNum: CompositeSeqInput, payload: string, isClient: boolean) {
    return new TestEvent({
      seqNum,
      parentSeqNum,
      name: 'test',
      args: payload,
      clientId: 'client-a',
      sessionId: 'session-a',
      isClient,
    })
  }

  rebaseFor(parentSeqNum = this.parentSeqNum, rebaseGeneration = 0) {
    return this.rebase({ parentSeqNum, isClient: this.isClient, rebaseGeneration })
  }
}

const e0_1 = TestEvent.make({ global: 0, client: 1 }, ROOT, 'a', true)
const e1_0 = TestEvent.make({ global: 1, client: 0 }, ROOT, 'a', false)
const e1_1 = TestEvent.make({ global: 1, client: 1 }, e1_0.seqNum, 'a', true)
const e1_2 = TestEvent.make({ global: 1, client: 2 }, e1_1.seqNum, 'a', true)
const e2_0 = TestEvent.make({ global: 2, client: 0 }, e1_0.seqNum, 'a', false)

const isClientEvent = (event: ClientEvent) => event instanceof TestEvent && event.isClient

const runMerge = (syncState: SyncState, payload: Parameters<typeof merge>[0]['payload'], ignoreClientEvents = false) =>
  Effect.runPromise(merge({ syncState, payload, isClientEvent, eventsEqual: isEqualEvent, ignoreClientEvents }))

const seqs = (events: readonly ClientEvent[]) => events.map((event) => seqToString(event.seqNum))

describe('sequence numbers', () => {
  it('computes global and client next pairs', () => {
    expect(nextSeqPair({ seq: ROOT, isClient: false }).seqNum).toStrictEqual({ global: 1, client: 0, rebaseGeneration: 0 })
    expect(nextSeqPair({ seq: ROOT, isClient: true }).seqNum).toStrictEqual({ global: 0, client: 1, rebaseGeneration: 0 })
    expect(nextSeqPair({ seq: ROOT, isClient: false, rebaseGeneration: 1 }).seqNum).toStrictEqual({
      global: 1,
      client: 0,
      rebaseGeneration: 1,
    })
  })

  it('round-trips readable notation', () => {
    for (const input of [
      { global: 0, client: 0, rebaseGeneration: 0 },
      { global: 0, client: 1, rebaseGeneration: 0 },
      { global: 0, client: 1, rebaseGeneration: 1 },
      { global: 5, client: 3, rebaseGeneration: 2 },
    ]) {
      const seq = makeSeq(input)
      expect(seqFromString(seqToString(seq))).toStrictEqual(seq)
    }
  })

  it('compares global, then client, then rebase generation', () => {
    expect(compareSeq(makeSeq({ global: 0, client: 1, rebaseGeneration: 1 }), makeSeq({ global: 1, client: 0 }))).toBeLessThan(0)
    expect(compareSeq(makeSeq({ global: 1, client: 0 }), makeSeq({ global: 0, client: 9 }))).toBeGreaterThan(0)
    expect(compareSeq(makeSeq({ global: 1, client: 0, rebaseGeneration: 1 }), makeSeq({ global: 1, client: 0 }))).toBeGreaterThan(0)
  })
})

describe('sync-state merge', () => {
  it('accepts a local push by appending it to pending', async () => {
    const result = await runMerge(
      { pending: [], upstreamHead: ROOT, localHead: ROOT },
      { _tag: 'local-push', newEvents: [e1_0] },
    )

    expect(result._tag).toBe('advance')
    if (result._tag !== 'advance') throw new Error('expected advance')
    expect(seqs(result.newSyncState.pending)).toStrictEqual(['e1'])
    expect(seqToString(result.newSyncState.localHead)).toBe('e1')
  })

  it('rejects a stale local push at or behind the local head', async () => {
    const result = await runMerge(
      { pending: [e1_0], upstreamHead: ROOT, localHead: e1_0.seqNum },
      { _tag: 'local-push', newEvents: [e1_0] },
    )

    expect(result._tag).toBe('reject')
    if (result._tag !== 'reject') throw new Error('expected reject')
    expect(seqToString(result.expectedMinimumId)).toBe('e1.1')
  })

  it('confirms matching pending events from upstream', async () => {
    const result = await runMerge(
      { pending: [e1_0, e2_0], upstreamHead: ROOT, localHead: e2_0.seqNum },
      { _tag: 'upstream-advance', newEvents: [e1_0] },
    )

    expect(result._tag).toBe('advance')
    if (result._tag !== 'advance') throw new Error('expected advance')
    expect(seqs(result.confirmedEvents)).toStrictEqual(['e1'])
    expect(seqs(result.newSyncState.pending)).toStrictEqual(['e2'])
    expect(result.newEvents).toHaveLength(0)
  })

  it('keeps client-only pending events out of leader backend confirmation comparisons', async () => {
    const result = await runMerge(
      { pending: [e0_1, e1_0], upstreamHead: ROOT, localHead: e1_0.seqNum },
      { _tag: 'upstream-advance', newEvents: [e1_0] },
      true,
    )

    expect(result._tag).toBe('advance')
    if (result._tag !== 'advance') throw new Error('expected advance')
    expect(seqs(result.confirmedEvents)).toStrictEqual(['e0.1', 'e1'])
    expect(result.newSyncState.pending).toHaveLength(0)
  })

  it('rebases divergent pending events on top of upstream advance', async () => {
    const remoteE1 = TestEvent.make({ global: 1, client: 0 }, ROOT, 'remote', false)
    const result = await runMerge(
      { pending: [e1_0, e1_1], upstreamHead: ROOT, localHead: e1_1.seqNum },
      { _tag: 'upstream-advance', newEvents: [remoteE1] },
    )

    expect(result._tag).toBe('rebase')
    if (result._tag !== 'rebase') throw new Error('expected rebase')
    expect(seqs(result.rollbackEvents)).toStrictEqual(['e1', 'e1.1'])
    expect(seqs(result.newSyncState.pending)).toStrictEqual(['e2r1', 'e2.1r1'])
    expect(seqs(result.newEvents)).toStrictEqual(['e1', 'e2r1', 'e2.1r1'])
  })

  it('applies explicit upstream rebase payloads and rolls back local pending after upstream rollback events', async () => {
    const rebasedE1 = e1_0.rebaseFor(e2_0.seqNum, 0)
    const result = await runMerge(
      { pending: [e2_0], upstreamHead: ROOT, localHead: e2_0.seqNum },
      { _tag: 'upstream-rebase', rollbackEvents: [e1_0], newEvents: [rebasedE1] },
    )

    expect(result._tag).toBe('rebase')
    if (result._tag !== 'rebase') throw new Error('expected rebase')
    expect(seqs(result.rollbackEvents)).toStrictEqual(['e1', 'e2'])
    expect(seqs(result.newSyncState.pending)).toStrictEqual(['e4r1'])
    expect(seqs(result.newEvents)).toStrictEqual(['e3', 'e4r1'])
  })

  it('dies on out-of-order upstream events', async () => {
    const exit = await Effect.runPromiseExit(
      merge({
        syncState: { pending: [e1_0], upstreamHead: ROOT, localHead: e1_0.seqNum },
        payload: { _tag: 'upstream-advance', newEvents: [e1_1, e1_0] },
        isClientEvent,
      }),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) expect(Cause.isDieType(exit.cause)).toBe(true)
  })

  it('treats undefined-valued args and JSON-round-tripped args as equal', async () => {
    const local = new ClientEvent({
      seqNum: e1_0.seqNum,
      parentSeqNum: e1_0.parentSeqNum,
      name: 'test',
      args: { id: 'abc', flag: undefined },
      clientId: 'client-a',
      sessionId: 'session-a',
    })
    const wire = new ClientEvent({
      seqNum: e1_0.seqNum,
      parentSeqNum: e1_0.parentSeqNum,
      name: 'test',
      args: JSON.parse(JSON.stringify(local.args)),
      clientId: 'client-a',
      sessionId: 'session-a',
    })

    const result = await runMerge(
      { pending: [local], upstreamHead: ROOT, localHead: local.seqNum },
      { _tag: 'upstream-advance', newEvents: [wire] },
    )

    expect(result._tag).toBe('advance')
    if (result._tag !== 'advance') throw new Error('expected advance')
    expect(result.confirmedEvents).toHaveLength(1)
    expect(result.newSyncState.pending).toHaveLength(0)
  })
})
