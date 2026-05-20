// Package factorial defines the recursive-factorial workflow shared by the
// worker and client binaries in this example.
package factorial

import (
	resonate "github.com/resonatehq/resonate-sdk-go"
)

// Name is the registered function name. Both the worker (Register) and the
// client (r.RPC) use this string.
const Name = "Factorial"

// WorkerGroup is the dispatch group the worker(s) join and the client targets
// when calling RPC. Using a non-default group keeps clients out of the
// task-dispatch pool — only processes that actually have Factorial registered
// will be asked to execute it.
const WorkerGroup = "factorial-workers"

// Args is the workflow input.
type Args struct {
	N int `json:"n"`
}

// Workflow computes n! by recursively dispatching factorial(n-1) via ctx.RPC.
// Each recursive call is a durable promise on the server; with multiple
// workers running, the recursion fans out across them.
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
