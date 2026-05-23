PROTOC ?= protoc
PROTO_FILES := $(shell find api/proto -name '*.proto')
MODULE := github.com/RishatShay/sna-final-project

.PHONY: test race build compose-up compose-down proto proto-tools

test:
	go test ./...

race:
	go test -race ./...

build:
	go build ./cmd/raftnode
	go build ./cmd/raftctl

compose-up:
	docker compose up --build

compose-down:
	docker compose down

proto:
	$(PROTOC) -I api/proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
