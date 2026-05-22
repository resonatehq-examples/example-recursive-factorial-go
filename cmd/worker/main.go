// Package main is the worker binary for the recursive-factorial example.
// It joins the `factorial-workers` group, registers the workflow under
// the shared name, and blocks until the process is interrupted. Run as
// many of these as you like — the server distributes recursive
// `ctx.RPC(factorial.Name, ...)` calls across all available workers.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"

	"github.com/resonatehq-examples/example-recursive-factorial-go/factorial"
)

func main() {
	url := flag.String("url", "http://localhost:8001", "Resonate server URL")
	flag.Parse()

	pid := fmt.Sprintf("factorial-worker-%d", os.Getpid())
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
			PID:   pid,
			Group: factorial.WorkerGroup,
		}),
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	if _, err := resonate.Register(r, factorial.Name, factorial.Workflow); err != nil {
		log.Fatalf("Register: %v", err)
	}

	fmt.Printf("[worker pid=%s group=%s] ready — waiting for tasks (Ctrl-C to exit)\n", pid, factorial.WorkerGroup)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("[worker] shutting down")
}
