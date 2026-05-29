import { Effect } from 'effect'

import { createStore, makeFifoBackend, type Reducer } from '../src/index.ts'

type TodoState = {
  todos: { id: string; text: string; selected?: boolean }[]
}

const eventDefs = [{ name: 'todo-created' }, { name: 'todo-selected', clientOnly: true }]

const reducers: Record<string, Reducer<TodoState>> = {
  'todo-created': (state, args) => {
    const todo = args as { id: string; text: string }
    return state.todos.some((existing) => existing.id === todo.id)
      ? state
      : { todos: [...state.todos, { id: todo.id, text: todo.text }] }
  },
  'todo-selected': (state, args) => {
    const selected = args as { id: string }
    return {
      todos: state.todos.map((todo) => (todo.id === selected.id ? { ...todo, selected: true } : todo)),
    }
  },
}

const backend = await Effect.runPromise(makeFifoBackend())

const firstTab = await createStore({
  eventDefs,
  reducers,
  initialState: { todos: [] },
  backend,
  clientId: 'client-a',
  sessionId: 'tab-a',
})

const secondTab = await createStore({
  eventDefs,
  reducers,
  initialState: { todos: [] },
  backend,
  clientId: 'client-b',
  sessionId: 'tab-b',
})

await firstTab.dispatch({ name: 'todo-created', args: { id: 'a', text: 'Read the sync-state layer' } })
await secondTab.dispatch({ name: 'todo-created', args: { id: 'b', text: 'Trace a rebase' } })
await firstTab.pull()

await firstTab.dispatch({ name: 'todo-selected', args: { id: 'a' } })

console.log('first tab state')
console.log(JSON.stringify(await firstTab.getState(), null, 2))

console.log('second tab state')
console.log(JSON.stringify(await secondTab.getState(), null, 2))

console.log('backend FIFO log')
console.log(JSON.stringify(await Effect.runPromise(backend.snapshot), null, 2))
