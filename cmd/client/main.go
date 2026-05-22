// Package main is the client binary for the recursive-factorial example.
// It dispatches a single `factorial(n)` invocation to the worker group
// via `r.RPC`, awaits the typed result, and prints it. The client does
// not register the workflow — that's the worker's job. The promise ID
// is stable (`factorial-<n>`), so a second run with the same -n returns
// the cached result instantly.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"

	"github.com/resonatehq-examples/example-recursive-factorial-go/factorial"
)

func main() {
	n := flag.Int("n", 6, "compute factorial(n)")
	url := flag.String("url", "http://localhost:8001", "Resonate server URL")
	flag.Parse()

	pid := fmt.Sprintf("factorial-client-%d", time.Now().UnixNano())
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
			PID:   pid,
			Group: "factorial-client",
		}),
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id := fmt.Sprintf("factorial-%d", *n)
	target := fmt.Sprintf("poll://any@%s", factorial.WorkerGroup)

	h, err := r.RPC(ctx, id, factorial.Name, factorial.Args{N: *n},
		resonate.RPCOptions{Target: target})
	if err != nil {
		log.Fatalf("RPC: %v", err)
	}

	var result int
	if err := h.Result(ctx, &result); err != nil {
		log.Fatalf("Result: %v", err)
	}
	fmt.Printf("factorial(%d) = %d\n", *n, result)
}
