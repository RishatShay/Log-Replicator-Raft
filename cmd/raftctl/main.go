// Command raftctl talks to the cluster: it writes and reads keys, prints node
// status and compares the replicated logs of several nodes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/RishatShay/sna-final-project/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultCluster matches the ports published by docker-compose.yml.
var defaultCluster = []string{
	"localhost:9001",
	"localhost:9002",
	"localhost:9003",
	"localhost:9004",
	"localhost:9005",
}

func main() {
	addr := flag.String("addr", defaultCluster[0], "gRPC address of the node to talk to")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	if err := execute(*addr, *timeout, args); err != nil {
		fmt.Fprintf(os.Stderr, "raftctl: %s\n", message(err))
		os.Exit(1)
	}
}

func execute(addr string, timeout time.Duration, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return run(ctx, addr, args)
}

func run(ctx context.Context, addr string, args []string) error {
	command, args := args[0], args[1:]
	switch command {
	case "write":
		if len(args) != 2 {
			return errors.New("usage: raftctl write <key> <value>")
		}
		return call(addr, func(client raftpb.ClientServiceClient) error {
			resp, err := client.Write(ctx, &raftpb.WriteRequest{Key: args[0], Value: args[1]})
			return printJSON(resp, err)
		})
	case "read":
		if len(args) != 1 {
			return errors.New("usage: raftctl read <key>")
		}
		return call(addr, func(client raftpb.ClientServiceClient) error {
			resp, err := client.Read(ctx, &raftpb.ReadRequest{Key: args[0]})
			return printJSON(resp, err)
		})
	case "delete":
		if len(args) != 1 {
			return errors.New("usage: raftctl delete <key>")
		}
		return call(addr, func(client raftpb.ClientServiceClient) error {
			resp, err := client.Delete(ctx, &raftpb.DeleteRequest{Key: args[0]})
			return printJSON(resp, err)
		})
	case "status":
		return call(addr, func(client raftpb.ClientServiceClient) error {
			resp, err := client.Status(ctx, &raftpb.StatusRequest{})
			return printJSON(resp, err)
		})
	case "log":
		return call(addr, func(client raftpb.ClientServiceClient) error {
			resp, err := client.InspectLog(ctx, &raftpb.InspectLogRequest{})
			if err != nil {
				return err
			}
			printLog(addr, resp)
			return nil
		})
	case "compare":
		addresses := args
		if len(addresses) == 0 {
			addresses = defaultCluster
		}
		return compare(ctx, addresses)
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

// compare fetches the log of every node and verifies that the committed part is
// identical everywhere.
func compare(ctx context.Context, addresses []string) error {
	reports := make(map[string]*raftpb.InspectLogResponse, len(addresses))
	for _, address := range addresses {
		err := call(address, func(client raftpb.ClientServiceClient) error {
			resp, err := client.InspectLog(ctx, &raftpb.InspectLogRequest{})
			if err != nil {
				return err
			}
			reports[address] = resp
			printLog(address, resp)
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s: %w", address, message(err))
		}
	}

	// Nodes may hold different uncommitted tails, so only the committed part of
	// the log has to match. Compacted entries are no longer in any log.
	from, through := comparableRange(reports)
	var reference []string
	var referenceAddress string
	for address, report := range reports {
		entries := describeEntries(report, from, through)
		if reference == nil {
			reference, referenceAddress = entries, address
			continue
		}
		if index, ok := firstDifference(reference, entries); ok {
			return fmt.Errorf("logs of %s and %s differ at entry %d", referenceAddress, address, from+uint64(index))
		}
	}

	fmt.Printf("\n%d nodes agree on entries %d..%d\n", len(reports), from, through)
	return nil
}

// comparableRange is the index window every node can be asked about: above the
// highest snapshot and up to the lowest commit index.
func comparableRange(reports map[string]*raftpb.InspectLogResponse) (from, through uint64) {
	through = ^uint64(0)
	for _, report := range reports {
		from = max(from, report.GetSnapshotIndex())
		through = min(through, report.GetCommitIndex())
	}
	return from + 1, through
}

func describeEntries(report *raftpb.InspectLogResponse, from, through uint64) []string {
	described := make([]string, 0, len(report.GetEntries()))
	for _, entry := range report.GetEntries() {
		if entry.GetIndex() < from || entry.GetIndex() > through {
			continue
		}
		described = append(described, fmt.Sprintf("%d/%d/%s", entry.GetIndex(), entry.GetTerm(), entry.GetCommand()))
	}
	return described
}

func firstDifference(left, right []string) (int, bool) {
	for i := range min(len(left), len(right)) {
		if left[i] != right[i] {
			return i, true
		}
	}
	if len(left) != len(right) {
		return min(len(left), len(right)), true
	}
	return 0, false
}

func call(addr string, fn func(client raftpb.ClientServiceClient) error) error {
	conn, err := grpc.NewClient(dialTarget(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close()

	return fn(raftpb.NewClientServiceClient(conn))
}

func printJSON(value any, err error) error {
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printLog(address string, report *raftpb.InspectLogResponse) {
	fmt.Printf("\n%s (%s) role=%s term=%d commit=%d applied=%d snapshot=%d\n",
		report.GetId(), address, report.GetRole(), report.GetTerm(),
		report.GetCommitIndex(), report.GetLastApplied(), report.GetSnapshotIndex())

	for _, entry := range report.GetEntries() {
		committed := " "
		if entry.GetIndex() <= report.GetCommitIndex() {
			committed = "*"
		}
		fmt.Printf("  %s %4d term=%d %s\n", committed, entry.GetIndex(), entry.GetTerm(), entry.GetCommand())
	}
	for _, key := range slices.Sorted(maps.Keys(report.GetState())) {
		fmt.Printf("    %s = %s\n", key, report.GetState()[key])
	}
}

// dialTarget keeps plain host:port addresses working with the gRPC resolver.
func dialTarget(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "passthrough:///" + address
}

// message unwraps a gRPC status so the CLI prints "not the leader" instead of
// "rpc error: code = Unavailable desc = not the leader".
func message(err error) error {
	if err == nil {
		return nil
	}
	if grpcStatus, ok := status.FromError(err); ok {
		return errors.New(grpcStatus.Message())
	}
	return err
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: raftctl [-addr host:port] [-timeout 5s] <command>

commands:
  write <key> <value>       append a set command and wait for a majority
  read <key>                read a key from the leader
  delete <key>              append a delete command and wait for a majority
  status                    print the Raft state of one node
  log                       print the log and state machine of one node
  compare [addr ...]        check that all nodes hold the same committed log

examples:
  raftctl -addr localhost:9001 write course sna
  raftctl -addr localhost:9002 read course
  raftctl compare localhost:9001 localhost:9002 localhost:9003
`)
}
