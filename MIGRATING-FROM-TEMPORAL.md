# Coming from Temporal: Recursive / Child Workflows

This guide maps [`temporalio/samples-go/child-workflow`](https://github.com/temporalio/samples-go/tree/main/child-workflow)
(and its history-management companion [`child-workflow-continue-as-new`](https://github.com/temporalio/samples-go/tree/main/child-workflow-continue-as-new))
to this Resonate example. The goal is to help you port the recursive-invocation
pattern — not rewrite from scratch.

## The pattern

Both systems make it possible to invoke a workflow function from within a running
workflow execution, creating a tree of durable sub-calls. In Temporal this is done
through a dedicated child-workflow mechanism. In Resonate a registered function
calls itself by name through `ctx.RPC`, and the result is a durable promise on the
server — no separate primitive is needed.

## Side by side

### Temporal (`samples-go/child-workflow`) — parent

`child-workflow/parent_workflow.go`:

```go
// (logger calls omitted for brevity)
func SampleParentWorkflow(ctx workflow.Context) (string, error) {
    cwo := workflow.ChildWorkflowOptions{
        WorkflowID: "ABC-SIMPLE-CHILD-WORKFLOW-ID",
    }
    ctx = workflow.WithChildOptions(ctx, cwo)

    var result string
    err := workflow.ExecuteChildWorkflow(ctx, SampleChildWorkflow, "World").Get(ctx, &result)
    if err != nil {
        return "", err
    }
    return result, nil
}
```

### Resonate (this example) — `factorial/factorial.go`

```go
func Workflow(ctx *resonate.Context, args Args) (int, error) {
    if args.N <= 1 {
        return 1, nil
    }
    f, err := ctx.RPC(Name, Args{N: args.N - 1})
    if err != nil {
        return 0, err
    }
    var sub int
    if err := f.Await(&sub); err != nil {
        return 0, err
    }
    return args.N * sub, nil
}
```

The call site in the worker and client binaries:

```go
// cmd/worker/main.go — registers the function into the named group
network := httpnet.NewHTTP("http://localhost:8001", httpnet.HTTPOptions{
    Group: factorial.WorkerGroup,
})
r, err := resonate.New(resonate.Config{Network: network})
if err != nil {
    log.Fatalf("resonate.New: %v", err)
}
if _, err := resonate.Register(r, factorial.Name, factorial.Workflow); err != nil {
    log.Fatalf("Register: %v", err)
}
```

```go
// cmd/client/main.go — invokes by name, targeting the worker group
id := fmt.Sprintf("factorial-%d", *n)
h, err := r.RPC(ctx, id, factorial.Name, factorial.Args{N: *n},
    resonate.RPCOptions{Target: factorial.WorkerGroup})
if err != nil {
    log.Fatalf("RPC: %v", err)
}

var result int
if err := h.Result(ctx, &result); err != nil {
    log.Fatalf("Result: %v", err)
}
fmt.Printf("factorial(%d) = %d\n", *n, result)
```

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.ExecuteChildWorkflow(ctx, Fn, args)` | `ctx.RPC(Name, args)` | Returns a future in both cases; `ctx.RPC` uses the registered string name |
| `.Get(ctx, &result)` | `f.Await(&result)` | Blocks the current execution until the sub-call settles |
| `workflow.ChildWorkflowOptions{WorkflowID: id}` | Promise ID embedded in `id` arg to `r.RPC` | Stable ID = idempotency; re-running with the same ID returns the cached result |
| `workflow.WithChildOptions(ctx, cwo)` | No equivalent needed | Options are passed inline to `ctx.RPC` / `r.RPC` |
| Task queue (registered on worker, targeted by parent) | Worker group (`factorial.WorkerGroup`) | Named group routes dispatches to registered workers only |
| `w.RegisterWorkflow(Fn)` | `resonate.Register(r, Name, Fn)` | Name is a string; worker and client share it via a package constant |
| `workflow.NewContinueAsNewError(...)` | Not needed | See below |

## Porting it, step by step

1. **Flatten the child-workflow type.** In Temporal, a child workflow is a distinct
   workflow execution, started with `ExecuteChildWorkflow`. In Resonate, the same
   function simply calls `ctx.RPC(Name, args)` with its own registered name. Delete
   the parent/child split and write one function.

2. **Replace `ExecuteChildWorkflow(...).Get(ctx, &r)` with `ctx.RPC` + `f.Await`.**
   `ctx.RPC(Name, Args{N: args.N-1})` returns a `(Future, error)`. Call `f.Await(&sub)`
   where you previously called `.Get(ctx, &result)`.

3. **Move the workflow ID into the top-level invocation.** In Temporal you set
   `ChildWorkflowOptions.WorkflowID` per child call. In Resonate the stable ID
   travels with the root invocation (`id := fmt.Sprintf("factorial-%d", *n)`) and
   the SDK generates deterministic child IDs (worker-side) (`factorial-6.1`, `factorial-6.1.1`,
   …) automatically.

4. **Register once on the worker; do not register on the client.** Call
   `resonate.Register(r, factorial.Name, factorial.Workflow)` in the worker binary.
   The client binary constructs a `resonate.New` without a `Network` (just a `URL`)
   and never calls `Register`. Using a named group (`factorial.WorkerGroup`) keeps
   the client out of the task-dispatch pool, which avoids "function not found" errors
   if the server tries to route a recursive step to the client process.

5. **Drop ContinueAsNew entirely.** If you were using `child-workflow-continue-as-new`
   to bound history depth, you do not need an equivalent. See the next section for why.

## What's different (and why)

### Why Temporal has child workflows and ContinueAsNew

Temporal's execution model is event-history replay: when a worker restarts, it
replays the full event history of a workflow execution to reconstruct its in-memory
state. That history is stored per workflow execution and grows with every event.

Deep or unbounded recursion in a single workflow execution would grow the history
without bound, so Temporal models each recursive call as a separate child workflow
execution with its own history. For truly unbounded repetition (e.g. a counter that
must run N times without knowing N up front), `workflow.NewContinueAsNewError`
closes the current execution and starts a fresh one with a clean history, carrying
forward only the arguments you explicitly pass.

`child-workflow-continue-as-new/child_workflow.go` illustrates this:

```go
// (input guard omitted for brevity)
func SampleChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
    totalCount++
    runCount--
    if runCount == 0 {
        result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
        return result, nil
    }
    return "", workflow.NewContinueAsNewError(ctx, SampleChildWorkflow, totalCount, runCount)
}
```

Each iteration closes the execution and opens a new one, keeping history bounded.

### How Resonate handles the same problem

Resonate does not replay event history to restore state. Each `ctx.RPC` call
creates a durable promise on the server. If a worker crashes mid-execution, the
server re-dispatches the promise to any available worker in the group; the function
re-runs from the top of its body (not a replay). Because there is no per-execution
history accumulating, deep recursion does not cause a history-size problem. A
registered function calls itself by name directly — no child-workflow type, no
ContinueAsNew, no history-boundary accounting.

The tradeoff is different: in Resonate you get at-least-once re-execution of a
function body after a crash rather than deterministic history replay, so any
side effects in your function body need to be idempotent (or guarded by `ctx.Run`
/ additional durable promises).

### Worker groups vs. task queues

Temporal routes work to workers via task queues, named at worker startup and
referenced in activity/child-workflow options. Resonate uses a similar concept
called a group: the worker joins `factorial.WorkerGroup` via `httpnet.HTTPOptions`,
and the client targets the same group with `resonate.RPCOptions{Target: factorial.WorkerGroup}`.
A key difference: in this example the client is a separate binary that does
**not** register the workflow function. Without a named group, the server could
dispatch recursive steps to the client process and fail. The group name is a
package constant (`factorial.WorkerGroup = "factorial-workers"`) shared between
both binaries.

### Stable IDs and idempotency

The promise ID (`factorial-6`) is passed by the client and stored on the server.
Re-running the client with `-n 6` a second time returns the already-settled result
instantly — the server recognises the ID and returns the cached value rather than
re-dispatching. This is a first-class feature, not a workaround. In Temporal,
idempotency must be configured explicitly at the call site via
`StartWorkflowOptions.WorkflowIDReusePolicy` (or `WorkflowIDConflictPolicy`, e.g.
`UseExisting`); in Resonate it is the unconditional default.

## Notes & coverage

- **No `@workflow`/`@activity` split.** Resonate has no decorator-based distinction
  between workflow functions and activity functions. Any function registered with
  `resonate.Register` is a durable unit; calling `ctx.RPC` inside it makes the
  recursive sub-call durable. There are no timeout options required for the
  recursive call itself.
- **Promise ID namespace.** The SDK generates child IDs worker-side automatically
  (visible on the dashboard at `http://localhost:8001`). You do not need to
  manage them manually the way you would with `ChildWorkflowOptions.WorkflowID`.
- **Pre-release SDK.** `resonate-sdk-go` has no semver tag yet. This example
  pins to a specific commit; expect API changes until `v0.1.0`.
- **At-least-once semantics.** Because Resonate re-dispatches rather than replays,
  if a worker crashes after a side effect but before the promise settles, the side
  effect may run again. Design functions to be idempotent or confine side effects
  to their own durable promises.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample (child-workflow): https://github.com/temporalio/samples-go/tree/main/child-workflow
- Temporal sample (child-workflow-continue-as-new): https://github.com/temporalio/samples-go/tree/main/child-workflow-continue-as-new
- This example's README: [README.md](README.md)
- Durable promises: https://docs.resonatehq.io/learn/durable-promises
