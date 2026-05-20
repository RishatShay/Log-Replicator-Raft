package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RishatShay/sna-final-project/internal/config"
	"github.com/RishatShay/sna-final-project/internal/raft"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	node, err := raft.New(raft.OptionsFromConfig(cfg))
	if err != nil {
		fmt.Fprintf(os.Stderr, "node init error: %v\n", err)
		os.Exit(1)
	}
	if err := node.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "node start error: %v\n", err)
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "node stop error: %v\n", err)
		os.Exit(1)
	}
}
