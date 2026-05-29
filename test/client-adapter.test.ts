import { Effect } from 'effect'
import { describe, expect, it } from 'vitest'

import { createStore } from '../src/adapter.ts'
import { makeFifoBackend } from '../src/fifo-backend.ts'
import { makeLeader } from '../src/leader.ts'
import { makeClient, type Reducer } from '../src/client.ts'
import type { ClientEvent } from '../src/sync-state.ts'

type TodoState = {
  todos: { id: string; text: string; selected?: boolean }[]
}

const initialState: TodoState = { todos: [] }
const eventDefs = [{ name: 'todo-created' }, { name: 'todo-selected', clientOnly: true }]

const reducers: Record<string, Reducer<TodoState>> = {
  'todo-created': (state, args) => {
    const todo = args as { id: string; text: string }
    if (state.todos.some((existing) => existing.id === todo.id)) return state
    return { todos: [...state.todos, todo] }
  },
  'todo-selected': (state, args) => {
    const selected = args as { id: string }
    return {
      todos: state.todos.map((todo) => (todo.id === selected.id ? { ...todo, selected: true } : todo)),
    }
  },
}

describe('client and adapter', () => {
  it('creates a plain TypeScript store that can dispatch and read state', async () => {
    const store = await createStore({ eventDefs, reducers, initialState })

    await store.dispatch({ name: 'todo-created', args: { id: 'a', text: 'A' } })

    expect(await store.getState()).toStrictEqual({ todos: [{ id: 'a', text: 'A' }] })
    expect((await store.getSyncState()).pending).toHaveLength(0)
  })

  it('rebases a stale client push on top of another client through the same leader', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* makeFifoBackend()
        const leader = yield* makeLeader({ backend, eventDefs })
        const clientA = yield* makeClient({
          leader,
          eventDefs,
          reducers,
          initialState,
          clientId: 'client-a',
          sessionId: 'session-a',
        })
        const clientB = yield* makeClient({
          leader,
          eventDefs,
          reducers,
          initialState,
          clientId: 'client-b',
          sessionId: 'session-b',
        })

        yield* clientA.push([{ name: 'todo-created', args: { id: 'a', text: 'A' } }])
        yield* clientB.push([{ name: 'todo-created', args: { id: 'b', text: 'B' } }])
        yield* clientA.pullFromLeader

        return {
          clientAState: yield* clientA.state,
          clientBState: yield* clientB.state,
          clientBEvents: yield* clientB.eventlog,
          backendEvents: yield* backend.snapshot,
        }
      }),
    )

    expect(result.clientAState.todos.map((todo) => todo.id)).toStrictEqual(['a', 'b'])
    expect(result.clientBState.todos.map((todo) => todo.id)).toStrictEqual(['a', 'b'])
    expect(result.backendEvents.map((event) => [event.seqNum, (event.args as { id: string }).id])).toStrictEqual([
      [1, 'a'],
      [2, 'b'],
    ])
    expect((result.clientBEvents.at(-1) as ClientEvent).seqNum.rebaseGeneration).toBe(1)
  })

  it('materializes client-only events locally without syncing them to the backend', async () => {
    const store = await createStore({ eventDefs, reducers, initialState })

    await store.dispatch({ name: 'todo-created', args: { id: 'a', text: 'A' } })
    await store.dispatch({ name: 'todo-selected', args: { id: 'a' } })

    expect(await store.getState()).toStrictEqual({ todos: [{ id: 'a', text: 'A', selected: true }] })
    const backendEvents = await Effect.runPromise(store.backend.snapshot)
    expect(backendEvents.map((event) => event.name)).toStrictEqual(['todo-created'])
  })
})
