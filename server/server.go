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

	id          int
	peers       []string // addresses of the other nodes
	primaryAddr string

	mu            sync.Mutex
	isPrimary     bool
	roleMu        sync.RWMutex
	seq           int64
	highestBidder string
	highestBid    int64
	bidders       map[string]int64 // bidder -> their highest bid
	endTime       time.Time
}

// Helper to check if a node is primary

func (n *Node) primary() bool {
	n.roleMu.RLock()
	defer n.roleMu.RUnlock()
	return n.isPrimary
}

// ---------- Client-facing API ----------

func (n *Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	if !n.primary() {
		conn, err := grpc.NewClient(
			n.primaryAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			return &pb.BidResponse{
				Outcome: pb.Outcome_EXCEPTION,
				Message: "could not contact primary",
			}, nil
		}
		defer conn.Close()

		primary := pb.NewAuctionClient(conn)

		resp, err := primary.Bid(ctx, req)
		if err != nil {
			return &pb.BidResponse{
				Outcome: pb.Outcome_EXCEPTION,
				Message: "primary unavailable",
			}, nil
		}

		return resp, nil

	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if time.Now().After(n.endTime) {
		return &pb.BidResponse{
			Outcome: pb.Outcome_FAIL,
			Message: "auction is over",
		}, nil
	}
	if req.Amount <= n.highestBid {
		return &pb.BidResponse{
			Outcome: pb.Outcome_FAIL,
			Message: "bid too low",
		}, nil
	}

	// First bid registers the bidder.
	n.bidders[req.Bidder] = req.Amount
	n.highestBid = req.Amount
	n.highestBidder = req.Bidder
	n.seq++

	if !n.replicateToBackups() {
		return &pb.BidResponse{
			Outcome: pb.Outcome_EXCEPTION,
			Message: "could not replicate bid to backup",
		}, nil
	}

	return &pb.BidResponse{Outcome: pb.Outcome_SUCCESS}, nil
}

// Sends current state to all backups. A peer that doesn't answer within
// the timeout is treated as crashed (allowed: we tolerate 1 failure).
func (n *Node) replicateToBackups() bool {
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
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := pb.NewAuctionClient(conn).Replicate(ctx, state)
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("backup %s unreachable: %v", addr, err)
			return false
		}

		if !resp.Ok {
			log.Printf("backup %s did not acknowledge replication", addr)
			return false
		}
	}

	return true
}

func (n *Node) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	if !n.primary() {
		conn, err := grpc.NewClient(
			n.primaryAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		return pb.NewAuctionClient(conn).Result(ctx, req)
	}

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
	return &pb.HeartbeatResponse{PrimaryId: int32(n.id)}, nil
}

func (n *Node) monitorPrimary() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if n.primary() {
			return
		}

		conn, err := grpc.NewClient(
			n.primaryAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = pb.NewAuctionClient(conn).Heartbeat(ctx, &pb.HeartbeatRequest{})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf(" Primary is unreachable. Node %d is taking its place", n.id)

			n.roleMu.Lock()
			n.isPrimary = true
			n.roleMu.Unlock()

			return
		}

	}
}

// primary; if it doesn't answer within the known time-limit, take over.

func main() {
	id := flag.Int("id", 0, "node id (0 = initial primary)")
	port := flag.Int("port", 50051, "port to listen on")
	peers := flag.String("peers", "", "comma-separated addresses of other nodes, e.g. localhost:50052,localhost:50053")
	duration := flag.Duration("duration", 100*time.Second, "how long the auction runs")
	primary := flag.String("primary", "localhost:50051", "Address of the primary node")
	flag.Parse()

	node := &Node{
		id:          *id,
		isPrimary:   *id == 0,
		primaryAddr: *primary,
		bidders:     make(map[string]int64),
		endTime:     time.Now().Add(*duration),
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

	if !node.primary() {
		go node.monitorPrimary()
	}

	log.Printf("node %d listening on :%d (primary=%v, peers=%v)", node.id, *port, node.isPrimary, node.peers)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
