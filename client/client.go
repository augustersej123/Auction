package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	name := flag.String("name", "anon", "bidder name")
	addrs := flag.String("nodes", "localhost:50051,localhost:50052", "comma-separated node addresses")
	flag.Parse()

	nodes := strings.Split(*addrs, ",")
	current := 0

	connect := func() pb.AuctionClient {
		conn, err := grpc.NewClient(nodes[current], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("could not connect: %v", err)
		}
		return pb.NewAuctionClient(conn)
	}
	client := connect()

	fmt.Println("Commands: bid <amount> | result | quit")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		switch parts[0] {
		case "bid":
			if len(parts) != 2 {
				fmt.Println("usage: bid <amount>")
				break
			}
			amount, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("amount must be a number")
				break
			}
			resp, err := client.Bid(ctx, &pb.BidRequest{Bidder: *name, Amount: amount})
			if err != nil {
				// TODO: on failure, switch to the next node and retry.
				fmt.Println("exception:", err)
				break
			}
			fmt.Println(resp.Outcome, resp.Message)

		case "result":
			resp, err := client.Result(ctx, &pb.ResultRequest{})
			if err != nil {
				fmt.Println("exception:", err)
				break
			}
			if resp.Over {
				fmt.Printf("Auction over. Winner: %s with %d\n", resp.HighestBidder, resp.HighestBid)
			} else {
				fmt.Printf("Highest bid: %d by %s\n", resp.HighestBid, resp.HighestBidder)
			}

		case "quit":
			cancel()
			return

		default:
			fmt.Println("unknown command")
		}
		cancel()
	}
}
