// Command pulseq is the single binary that carries the whole orchestrator.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/a-holm/pulseq/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.Main(ctx, os.Args[1:]))
}
