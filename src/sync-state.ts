import { Effect } from 'effect'

export type GlobalSeq = number
export type ClientSeq = number

export type CompositeSeq = {
  global: GlobalSeq
  client: ClientSeq
  rebaseGeneration: number
}

export type CompositeSeqInput = CompositeSeq | { global: number; client: number; rebaseGeneration?: number }

export const makeSeq = (input: CompositeSeqInput): CompositeSeq => ({
  global: input.global,
  client: input.client,
  rebaseGeneration: input.rebaseGeneration ?? 0,
})

export const ROOT: CompositeSeq = { global: 0, client: 0, rebaseGeneration: 0 }

export const compareSeq = (a: CompositeSeq, b: CompositeSeq): number => {
  if (a.global !== b.global) return a.global - b.global
  if (a.client !== b.client) return a.client - b.client
  return a.rebaseGeneration - b.rebaseGeneration
}

export const seqToString = (seq: CompositeSeq): string => {
  const rebase = seq.rebaseGeneration > 0 ? `r${seq.rebaseGeneration}` : ''
  return seq.client === 0 ? `e${seq.global}${rebase}` : `e${seq.global}.${seq.client}${rebase}`
}

export const seqFromString = (value: string): CompositeSeq => {
  if (value.startsWith('e') === false) {
    throw new Error('Invalid event sequence number string: must start with "e"')
  }

  let body = value.slice(1)
  let rebaseGeneration = 0
  const rebaseMatch = body.match(/r(\d+)$/)
  if (rebaseMatch !== null) {
    rebaseGeneration = Number.parseInt(rebaseMatch[1]!, 10)
    body = body.slice(0, -rebaseMatch[0].length)
  }

  const parts = body.split('.')
  if (parts[0] === '' || /^\d+$/.test(parts[0]!) === false) {
    throw new Error('Invalid event sequence number string: invalid number format')
  }
  if (parts.length > 1 && (parts[1] === undefined || parts[1] === '' || /^\d+$/.test(parts[1]) === false)) {
    throw new Error('Invalid event sequence number string: invalid number format')
  }

  return {
    global: Number.parseInt(parts[0]!, 10),
    client: parts[1] === undefined ? 0 : Number.parseInt(parts[1], 10),
    rebaseGeneration,
  }
}

export const seqEqual = (a: CompositeSeq, b: CompositeSeq): boolean =>
  a.global === b.global && a.client === b.client && a.rebaseGeneration === b.rebaseGeneration

export const seqGreaterThan = (a: CompositeSeq, b: CompositeSeq): boolean =>
  a.global > b.global || (a.global === b.global && a.client > b.client)

export const seqGreaterThanOrEqual = (a: CompositeSeq, b: CompositeSeq): boolean =>
  a.global > b.global || (a.global === b.global && a.client >= b.client)

export const seqMax = (a: CompositeSeq, b: CompositeSeq): CompositeSeq => (seqGreaterThan(a, b) ? a : b)

export const seqFromGlobal = (global: GlobalSeq): CompositeSeq => ({
  global,
  client: 0,
  rebaseGeneration: 0,
})

export const nextSeqPair = ({
  seq,
  isClient,
  rebaseGeneration,
}: {
  seq: CompositeSeq
  isClient: boolean
  rebaseGeneration?: number
}): { seqNum: CompositeSeq; parentSeqNum: CompositeSeq } => {
  if (isClient === true) {
    return {
      seqNum: {
        global: seq.global,
        client: seq.client + 1,
        rebaseGeneration: rebaseGeneration ?? seq.rebaseGeneration,
      },
      parentSeqNum: seq,
    }
  }

  return {
    seqNum: {
      global: seq.global + 1,
      client: 0,
      rebaseGeneration: rebaseGeneration ?? seq.rebaseGeneration,
    },
    parentSeqNum: { global: seq.global, client: 0, rebaseGeneration: seq.rebaseGeneration },
  }
}

export type SessionChangeset =
  | { _tag: 'sessionChangeset'; data: Uint8Array; debug?: unknown }
  | { _tag: 'no-op' }
  | { _tag: 'unset' }

export type EventMeta = {
  sessionChangeset: SessionChangeset
  syncMetadata: unknown
  materializerHashLeader: number | undefined
  materializerHashSession: number | undefined
}

export type ClientEventInit = {
  name: string
  args: unknown
  seqNum: CompositeSeqInput
  parentSeqNum: CompositeSeqInput
  clientId: string
  sessionId: string
  meta?: Partial<EventMeta>
}

export type GlobalEvent = {
  name: string
  args: unknown
  seqNum: GlobalSeq
  parentSeqNum: GlobalSeq
  clientId: string
  sessionId: string
}

export class ClientEvent {
  readonly name: string
  readonly args: unknown
  readonly seqNum: CompositeSeq
  readonly parentSeqNum: CompositeSeq
  readonly clientId: string
  readonly sessionId: string
  readonly meta: EventMeta

  constructor(input: ClientEventInit) {
    this.name = input.name
    this.args = input.args
    this.seqNum = makeSeq(input.seqNum)
    this.parentSeqNum = makeSeq(input.parentSeqNum)
    this.clientId = input.clientId
    this.sessionId = input.sessionId
    this.meta = {
      sessionChangeset: input.meta?.sessionChangeset ?? { _tag: 'unset' },
      syncMetadata: input.meta?.syncMetadata,
      materializerHashLeader: input.meta?.materializerHashLeader,
      materializerHashSession: input.meta?.materializerHashSession,
    }
  }

  rebase(input: { parentSeqNum: CompositeSeq; isClient: boolean; rebaseGeneration: number }): ClientEvent {
    return new ClientEvent({
      name: this.name,
      args: this.args,
      clientId: this.clientId,
      sessionId: this.sessionId,
      ...nextSeqPair({
        seq: input.parentSeqNum,
        isClient: input.isClient,
        rebaseGeneration: input.rebaseGeneration,
      }),
      meta: { ...this.meta, sessionChangeset: { _tag: 'unset' } },
    })
  }

  toGlobal(): GlobalEvent {
    return {
      name: this.name,
      args: this.args,
      seqNum: this.seqNum.global,
      parentSeqNum: this.parentSeqNum.global,
      clientId: this.clientId,
      sessionId: this.sessionId,
    }
  }

  toJSON(): unknown {
    return {
      seqNum: `${seqToString(this.seqNum)} -> ${seqToString(this.parentSeqNum)} (${this.clientId}, ${this.sessionId})`,
      name: this.name,
      args: this.args,
    }
  }

  static fromGlobal(event: GlobalEvent, meta: Partial<EventMeta> = {}): ClientEvent {
    return new ClientEvent({
      ...event,
      seqNum: seqFromGlobal(event.seqNum),
      parentSeqNum: seqFromGlobal(event.parentSeqNum),
      meta,
    })
  }
}

const canonicalizeArgs = (args: unknown): unknown => (args === undefined ? args : JSON.parse(JSON.stringify(args)))

export const isEqualEvent = (a: ClientEvent, b: ClientEvent): boolean =>
  a.seqNum.global === b.seqNum.global &&
  a.seqNum.client === b.seqNum.client &&
  a.name === b.name &&
  a.clientId === b.clientId &&
  a.sessionId === b.sessionId &&
  JSON.stringify(canonicalizeArgs(a.args)) === JSON.stringify(canonicalizeArgs(b.args))

export type SyncState = {
  pending: readonly ClientEvent[]
  upstreamHead: CompositeSeq
  localHead: CompositeSeq
}

export const makeSyncState = (input?: Partial<SyncState>): SyncState => ({
  pending: input?.pending ?? [],
  upstreamHead: input?.upstreamHead ?? ROOT,
  localHead: input?.localHead ?? ROOT,
})

export type PayloadUpstreamRebase = {
  _tag: 'upstream-rebase'
  rollbackEvents: readonly ClientEvent[]
  newEvents: readonly ClientEvent[]
}

export type PayloadUpstreamAdvance = {
  _tag: 'upstream-advance'
  newEvents: readonly ClientEvent[]
}

export type PayloadLocalPush = {
  _tag: 'local-push'
  newEvents: readonly ClientEvent[]
}

export type PayloadUpstream = PayloadUpstreamRebase | PayloadUpstreamAdvance
export type Payload = PayloadUpstream | PayloadLocalPush

type MergeContext = {
  payload: Payload
  syncState: SyncState
}

export type MergeResultAdvance = {
  _tag: 'advance'
  newSyncState: SyncState
  newEvents: readonly ClientEvent[]
  confirmedEvents: readonly ClientEvent[]
  mergeContext: MergeContext
}

export type MergeResultRebase = {
  _tag: 'rebase'
  newSyncState: SyncState
  newEvents: readonly ClientEvent[]
  rollbackEvents: readonly ClientEvent[]
  mergeContext: MergeContext
}

export type MergeResultReject = {
  _tag: 'reject'
  expectedMinimumId: CompositeSeq
  mergeContext: MergeContext
}

export type MergeResult = MergeResultAdvance | MergeResultRebase | MergeResultReject

export const payloadFromMergeResult = (mergeResult: MergeResultAdvance | MergeResultRebase): PayloadUpstream =>
  mergeResult._tag === 'advance'
    ? { _tag: 'upstream-advance', newEvents: mergeResult.newEvents }
    : {
        _tag: 'upstream-rebase',
        newEvents: mergeResult.newEvents,
        rollbackEvents: mergeResult.rollbackEvents,
      }

export const merge = Effect.fnUntraced(function* ({
  syncState,
  payload,
  isClientEvent,
  eventsEqual = isEqualEvent,
  ignoreClientEvents = false,
}: {
  syncState: SyncState
  payload: Payload
  isClientEvent: (event: ClientEvent) => boolean
  eventsEqual?: (a: ClientEvent, b: ClientEvent) => boolean
  ignoreClientEvents?: boolean
}) {
  yield* validateSyncState(syncState)
  yield* validatePayload(payload)

  const mergeContext = { payload, syncState }

  switch (payload._tag) {
    case 'upstream-rebase': {
      const rollbackEvents = [...payload.rollbackEvents, ...syncState.pending]
      const newUpstreamHead = payload.newEvents.at(-1)?.seqNum ?? syncState.upstreamHead
      const rebasedPending = rebaseEvents({
        events: syncState.pending,
        baseSeq: newUpstreamHead,
        isClientEvent,
      })

      return yield* validateMergeResult({
        _tag: 'rebase',
        newSyncState: {
          pending: rebasedPending,
          upstreamHead: newUpstreamHead,
          localHead: rebasedPending.at(-1)?.seqNum ?? newUpstreamHead,
        },
        newEvents: [...payload.newEvents, ...rebasedPending],
        rollbackEvents,
        mergeContext,
      })
    }

    case 'upstream-advance': {
      if (payload.newEvents.length === 0) {
        return yield* validateMergeResult({
          _tag: 'advance',
          newSyncState: { ...syncState },
          newEvents: [],
          confirmedEvents: [],
          mergeContext,
        })
      }

      for (let i = 1; i < payload.newEvents.length; i++) {
        if (seqGreaterThan(payload.newEvents[i - 1]!.seqNum, payload.newEvents[i]!.seqNum)) {
          return yield* Effect.dieMessage(
            `Events must be sorted in ascending order by event number. Received: [${payload.newEvents
              .map((event) => seqToString(event.seqNum))
              .join(', ')}]`,
          )
        }
      }

      if (
        seqGreaterThan(syncState.upstreamHead, payload.newEvents[0]!.seqNum) ||
        seqEqual(syncState.upstreamHead, payload.newEvents[0]!.seqNum)
      ) {
        return yield* Effect.dieMessage(
          `Incoming events must be greater than upstream head. Expected greater than: ${seqToString(syncState.upstreamHead)}.`,
        )
      }

      const newUpstreamHead = payload.newEvents.at(-1)!.seqNum
      const divergentPendingIndex = findDivergencePoint({
        existingEvents: syncState.pending,
        incomingEvents: payload.newEvents,
        eventsEqual,
        isClientEvent,
        ignoreClientEvents,
      })

      if (divergentPendingIndex === -1) {
        const pendingSeqs = new Set(syncState.pending.map((event) => `${event.seqNum.global},${event.seqNum.client}`))
        const newEvents = payload.newEvents.filter((event) => !pendingSeqs.has(`${event.seqNum.global},${event.seqNum.client}`))

        let clientIndexOffset = 0
        const splitIndex = syncState.pending.findIndex((pendingEvent, index) => {
          if (ignoreClientEvents === true && isClientEvent(pendingEvent) === true) {
            clientIndexOffset++
            return false
          }

          const newEvent = payload.newEvents.at(index - clientIndexOffset)
          if (newEvent === undefined) return true
          return eventsEqual(pendingEvent, newEvent) === false
        })
        const pendingMatching = splitIndex === -1 ? syncState.pending : syncState.pending.slice(0, splitIndex)
        const pendingRemaining = splitIndex === -1 ? [] : syncState.pending.slice(splitIndex)

        return yield* validateMergeResult({
          _tag: 'advance',
          newSyncState: {
            pending: pendingRemaining,
            upstreamHead: newUpstreamHead,
            localHead: pendingRemaining.at(-1)?.seqNum ?? seqMax(syncState.localHead, newUpstreamHead),
          },
          newEvents,
          confirmedEvents: pendingMatching,
          mergeContext,
        })
      }

      const divergentPending = syncState.pending.slice(divergentPendingIndex)
      const rebasedPending = rebaseEvents({
        events: divergentPending,
        baseSeq: newUpstreamHead,
        isClientEvent,
      })

      const divergentNewEventsIndex = findDivergencePoint({
        existingEvents: payload.newEvents,
        incomingEvents: syncState.pending,
        eventsEqual,
        isClientEvent,
        ignoreClientEvents,
      })

      return yield* validateMergeResult({
        _tag: 'rebase',
        newSyncState: {
          pending: rebasedPending,
          upstreamHead: newUpstreamHead,
          localHead: rebasedPending.at(-1)!.seqNum,
        },
        newEvents: [...payload.newEvents.slice(divergentNewEventsIndex), ...rebasedPending],
        rollbackEvents: divergentPending,
        mergeContext,
      })
    }

    case 'local-push': {
      if (payload.newEvents.length === 0) {
        return yield* validateMergeResult({
          _tag: 'advance',
          newSyncState: syncState,
          newEvents: [],
          confirmedEvents: [],
          mergeContext,
        })
      }

      const first = payload.newEvents[0]!
      if (seqGreaterThan(first.seqNum, syncState.localHead) === false) {
        return yield* validateMergeResult({
          _tag: 'reject',
          expectedMinimumId: nextSeqPair({ seq: syncState.localHead, isClient: true }).seqNum,
          mergeContext,
        })
      }

      const nonClientEvents = ignoreClientEvents
        ? payload.newEvents.filter((event) => isClientEvent(event) === false)
        : payload.newEvents
      const newPending = [...syncState.pending, ...nonClientEvents]

      return yield* validateMergeResult({
        _tag: 'advance',
        newSyncState: {
          pending: newPending,
          upstreamHead: syncState.upstreamHead,
          localHead: newPending.at(-1)?.seqNum ?? seqMax(syncState.localHead, syncState.upstreamHead),
        },
        newEvents: payload.newEvents,
        confirmedEvents: [],
        mergeContext,
      })
    }
  }
})

export const findDivergencePoint = ({
  existingEvents,
  incomingEvents,
  eventsEqual,
  isClientEvent,
  ignoreClientEvents,
}: {
  existingEvents: readonly ClientEvent[]
  incomingEvents: readonly ClientEvent[]
  eventsEqual: (a: ClientEvent, b: ClientEvent) => boolean
  isClientEvent: (event: ClientEvent) => boolean
  ignoreClientEvents: boolean
}): number => {
  if (ignoreClientEvents === true) {
    const filteredExistingEvents = existingEvents.filter((event) => isClientEvent(event) === false)
    const divergencePointWithoutClientEvents = findDivergencePoint({
      existingEvents: filteredExistingEvents,
      incomingEvents,
      eventsEqual,
      isClientEvent,
      ignoreClientEvents: false,
    })

    if (divergencePointWithoutClientEvents === -1) return -1

    const divergentSeq = existingEvents[divergencePointWithoutClientEvents]!.seqNum
    return existingEvents.findIndex((event) => seqEqual(event.seqNum, divergentSeq))
  }

  return existingEvents.findIndex((existingEvent, index) => {
    const incomingEvent = incomingEvents[index]
    return incomingEvent !== undefined && eventsEqual(existingEvent, incomingEvent) === false
  })
}

const rebaseEvents = ({
  events,
  baseSeq,
  isClientEvent,
}: {
  events: readonly ClientEvent[]
  baseSeq: CompositeSeq
  isClientEvent: (event: ClientEvent) => boolean
}): readonly ClientEvent[] => {
  let prevSeq = baseSeq
  const rebaseGeneration = baseSeq.rebaseGeneration + 1

  return events.map((event) => {
    const nextEvent = event.rebase({
      parentSeqNum: prevSeq,
      isClient: isClientEvent(event),
      rebaseGeneration,
    })
    prevSeq = nextEvent.seqNum
    return nextEvent
  })
}

const validatePayload = (payload: Payload) =>
  Effect.gen(function* () {
    for (let i = 1; i < payload.newEvents.length; i++) {
      if (seqGreaterThanOrEqual(payload.newEvents[i - 1]!.seqNum, payload.newEvents[i]!.seqNum)) {
        return yield* Effect.dieMessage(
          `Events must be ordered in monotonically ascending order by eventNum. Received: [${payload.newEvents
            .map((event) => seqToString(event.seqNum))
            .join(', ')}]`,
        )
      }
    }
  })

const validateSyncState = Effect.fnUntraced(function* (syncState: SyncState) {
  for (let i = 0; i < syncState.pending.length; i++) {
    const event = syncState.pending[i]!
    const nextEvent = syncState.pending[i + 1]
    if (nextEvent === undefined) break

    if (seqGreaterThanOrEqual(event.seqNum, nextEvent.seqNum)) {
      return yield* Effect.dieMessage(
        `Events must be ordered in monotonically ascending order by eventNum. Received: [${syncState.pending
          .map((pendingEvent) => seqToString(pendingEvent.seqNum))
          .join(', ')}]`,
      )
    }

    if (nextEvent.seqNum.global > event.seqNum.global) {
      if (nextEvent.seqNum.client !== 0) {
        return yield* Effect.dieMessage(
          `New global events must point to clientId 0 in the parentSeqNum. Received: (${seqToString(nextEvent.seqNum)})`,
        )
      }
    } else if (seqEqual(nextEvent.parentSeqNum, event.seqNum) === false) {
      return yield* Effect.dieMessage('Events must be linked in a continuous chain via the parentSeqNum')
    }
  }
})

const validateMergeResult = Effect.fnUntraced(function* <T extends MergeResult>(mergeResult: T) {
  if (mergeResult._tag === 'reject') return mergeResult

  yield* validateSyncState(mergeResult.newSyncState)

  if (seqGreaterThan(mergeResult.newSyncState.upstreamHead, mergeResult.newSyncState.localHead)) {
    return yield* Effect.dieMessage('Local head must be greater than or equal to upstream head')
  }

  if (seqGreaterThanOrEqual(mergeResult.newSyncState.localHead, mergeResult.mergeContext.syncState.localHead) === false) {
    return yield* Effect.dieMessage('New local head must be greater than or equal to the previous local head')
  }

  if (
    seqGreaterThanOrEqual(mergeResult.newSyncState.upstreamHead, mergeResult.mergeContext.syncState.upstreamHead) ===
    false
  ) {
    return yield* Effect.dieMessage('New upstream head must be greater than or equal to the previous upstream head')
  }

  return mergeResult
})
