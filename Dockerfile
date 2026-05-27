FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# The SQLite driver is pure Go, so the binaries need no C toolchain at all.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/raftnode ./cmd/raftnode \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/raftctl ./cmd/raftctl

FROM alpine:3.20

COPY --from=build /out/raftnode /usr/local/bin/raftnode
COPY --from=build /out/raftctl /usr/local/bin/raftctl

# 9001 serves gRPC, 8001 serves /metrics and /healthz.
EXPOSE 9001 8001
ENTRYPOINT ["raftnode"]
