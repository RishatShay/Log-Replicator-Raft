GO ?= go
PROTOC ?= protoc
PROTO_FILES := $(shell find api/proto -name '*.proto')
MODULE := github.com/RishatShay/sna-final-project

.PHONY: help
help: ## Show the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-14s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build both binaries into bin/
	$(GO) build -o bin/ ./cmd/...

.PHONY: test
test: ## Run unit and integration tests
	$(GO) test ./...

.PHONY: race
race: ## Run the tests with the race detector
	$(GO) test -race ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format the code
	$(GO) fmt ./...

.PHONY: lint
lint: ## Run golangci-lint (requires golangci-lint)
	golangci-lint run

.PHONY: proto
proto: ## Regenerate the protobuf and gRPC code
	$(PROTOC) -I api/proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

.PHONY: proto-tools
proto-tools: ## Install the protoc plugins used by "make proto"
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

.PHONY: up
up: ## Start the five node cluster with Prometheus, Loki and Grafana
	docker compose up --build -d

.PHONY: down
down: ## Stop the stack and delete the volumes
	docker compose down -v

.PHONY: logs
logs: ## Follow the logs of every node
	docker compose logs -f node1 node2 node3 node4 node5

.PHONY: status
status: ## Print the Raft status of every node
	./scripts/demo.sh status

.PHONY: demo
demo: ## Print the demo script commands
	./scripts/demo.sh help

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin data
