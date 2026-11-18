package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Node is one replica of the auction.
type Node struct {
	pb.UnimplementedAuctionServer

	id    int
	peers []string // addresses of the other nodes

	mu            sync.Mutex
	isPrimary     bool
	seq           int64
	highestBidder string
	highestBid    int64
	bidders       map[string]int64 // bidder -> their highest bid
	endTime       time.Time
}

// ---------- Client-facing API ----------

func (n *Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// TODO: if this node is a backup, forward the bid to the primary
	// (or tell the client who the primary is).

	if time.Now().After(n.endTime) {
		return &pb.BidResponse{Outcome: pb.Outcome_FAIL, Message: "auction is over"}, nil
	}
	if req.Amount <= n.highestBid {
		return &pb.BidResponse{Outcome: pb.Outcome_FAIL, Message: "bid too low"}, nil
	}

	// First bid registers the bidder.
	n.bidders[req.Bidder] = req.Amount
	n.highestBid = req.Amount
	n.highestBidder = req.Bidder
	n.seq++

	n.replicateToBackups()
	return &pb.BidResponse{Outcome: pb.Outcome_SUCCESS}, nil
}

// Sends current state to all backups. A peer that doesn't answer within
// the timeout is treated as crashed (allowed: we tolerate 1 failure).
func (n *Node) replicateToBackups() {
	state := &pb.ReplicateRequest{
		Seq:           n.seq,
		HighestBidder: n.highestBidder,
		HighestBid:    n.highestBid,
		Bidders:       n.bidders,
		EndUnixMillis: n.endTime.UnixMilli(),
	}
	for _, addr := range n.peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if _, err := pb.NewAuctionClient(conn).Replicate(ctx, state); err != nil {
			log.Printf("backup %s unreachable: %v", addr, err)
		}
		cancel()
		conn.Close()
	}
}

func (n *Node) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return &pb.ResultResponse{
		Over:          time.Now().After(n.endTime),
		HighestBidder: n.highestBidder,
		HighestBid:    n.highestBid,
	}, nil
}

// ---------- Internal node-to-node API ----------

func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only apply newer state.
	if req.Seq > n.seq {
		n.seq = req.Seq
		n.highestBidder = req.HighestBidder
		n.highestBid = req.HighestBid
		n.bidders = make(map[string]int64, len(req.Bidders))
		for k, v := range req.Bidders {
			n.bidders[k] = v
		}
		n.endTime = time.UnixMilli(req.EndUnixMillis)
	}
	return &pb.ReplicateResponse{Ok: true}, nil
}

func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// TODO: return who this node believes is the primary.
	return &pb.HeartbeatResponse{PrimaryId: int32(n.id)}, nil
}

// TODO: monitorPrimary — backups periodically call Heartbeat on the
// primary; if it doesn't answer within the known time-limit, take over.

func main() {
	id := flag.Int("id", 0, "node id (0 = initial primary)")
	port := flag.Int("port", 50051, "port to listen on")
	peers := flag.String("peers", "", "comma-separated addresses of other nodes, e.g. localhost:50052,localhost:50053")
	duration := flag.Duration("duration", 100*time.Second, "how long the auction runs")
	flag.Parse()

	node := &Node{
		id:        *id,
		isPrimary: *id == 0,
		bidders:   make(map[string]int64),
		endTime:   time.Now().Add(*duration),
	}
	if *peers != "" {
		node.peers = strings.Split(*peers, ",")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterAuctionServer(s, node)

	log.Printf("node %d listening on :%d (primary=%v, peers=%v)", node.id, *port, node.isPrimary, node.peers)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
