# Distributed Auction

Primary–backup replicated auction using Go + gRPC.

## Layout

- `proto/auction.proto` – client API (`Bid`, `Result`) and internal API (`Replicate`, `Heartbeat`)
- `server/` – an auction node (run several as separate processes)
- `client/` – interactive bidder

## Regenerate gRPC code (after editing the proto)

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/auction.proto
```

## Run

One terminal per node:

```bash
go run ./server -id 0 -port 50051 -peers localhost:50052
```

```bash
go run ./server -id 1 -port 50052 -peers localhost:50051
```

Then a client:

```bash
go run ./client -name alice -nodes localhost:50051,localhost:50052
```

Client commands: `bid <amount>`, `result`, `quit`.
