package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/RishatShay/sna-final-project/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9001", "raft node gRPC address")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(wire.DialTarget(*addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	client := wire.NewClientServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch flag.Arg(0) {
	case "write":
		if flag.NArg() != 3 {
			usage()
			os.Exit(2)
		}
		resp, err := client.Write(ctx, &wire.ClientWriteRequest{Key: flag.Arg(1), Value: flag.Arg(2)})
		if err != nil {
			fatal(err)
		}
		printJSON(resp)
		if !resp.GetSuccess() {
			os.Exit(1)
		}
	case "read":
		if flag.NArg() != 2 {
			usage()
			os.Exit(2)
		}
		resp, err := client.Read(ctx, &wire.ClientReadRequest{Key: flag.Arg(1)})
		if err != nil {
			fatal(err)
		}
		printJSON(resp)
		if !resp.GetSuccess() {
			os.Exit(1)
		}
	case "status":
		resp, err := client.Status(ctx, &wire.StatusRequest{})
		if err != nil {
			fatal(err)
		}
		printJSON(resp)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  raftctl -addr localhost:9001 write <key> <value>
  raftctl -addr localhost:9001 read <key>
  raftctl -addr localhost:9001 status
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}
