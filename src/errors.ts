import type { CompositeSeq, GlobalSeq } from './sync-state.ts'
import { seqToString } from './sync-state.ts'

export class UnknownError extends Error {
  readonly _tag = 'UnknownError'
  readonly originalCause: unknown

  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause))
    this.originalCause = cause
  }
}

export class IsOfflineError extends Error {
  readonly _tag = 'IsOfflineError'
  readonly originalCause: unknown

  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause))
    this.originalCause = cause
  }
}

export class ServerAheadError extends Error {
  readonly _tag = 'ServerAheadError'

  constructor(
    readonly minimumExpectedNum: GlobalSeq,
    readonly providedNum: GlobalSeq,
  ) {
    super(`Backend head is ahead: expected parent ${minimumExpectedNum}, got ${providedNum}`)
  }
}

export class LeaderAheadError extends Error {
  readonly _tag = 'LeaderAheadError'

  constructor(
    readonly minimumExpectedNum: CompositeSeq,
    readonly providedNum: CompositeSeq,
    readonly sessionId: string,
  ) {
    super(
      `Leader push head is ahead of batch (session ${sessionId}): expected > ${seqToString(minimumExpectedNum)}, got ${seqToString(providedNum)}`,
    )
  }
}

export class NonMonotonicBatchError extends Error {
  readonly _tag = 'NonMonotonicBatchError'

  constructor(
    readonly precedingSeqNum: CompositeSeq,
    readonly violatingSeqNum: CompositeSeq,
    readonly violationIndex: number,
    readonly sessionId: string,
  ) {
    super(
      `Pushed events are not strictly increasing at index ${violationIndex} (session ${sessionId}): ${seqToString(precedingSeqNum)} >= ${seqToString(violatingSeqNum)}`,
    )
  }
}

export class StaleRebaseGenerationError extends Error {
  readonly _tag = 'StaleRebaseGenerationError'

  constructor(
    readonly currentRebaseGeneration: number,
    readonly providedRebaseGeneration: number,
    readonly sessionId: string,
  ) {
    super(
      `Pushed events have stale rebase generation (session ${sessionId}): expected >= ${currentRebaseGeneration}, got ${providedRebaseGeneration}`,
    )
  }
}

export type RejectedPushError = LeaderAheadError | NonMonotonicBatchError | StaleRebaseGenerationError

export const isRejectedPushError = (error: unknown): error is RejectedPushError =>
  error instanceof LeaderAheadError ||
  error instanceof NonMonotonicBatchError ||
  error instanceof StaleRebaseGenerationError
