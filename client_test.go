package main

import (
	"context"
	"io"
	"testing"

	"ApexTrade/routeguide"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCBridge(t *testing.T) {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to Python server: %v", err)
	}
	defer conn.Close()

	client := routeguide.NewRouteGuideClient(conn)

	stream, err := client.SendData(context.Background())
	if err != nil {
		t.Fatalf("Error opening gRPC stream: %v", err)
	}

	waitc := make(chan struct{})

	go func() {
		for {
			response, err := stream.Recv()
			
			if err == io.EOF {
				close(waitc) // Signal the main thread to exit
				return
			}
			if err != nil {
				t.Errorf("Stream read failed: %v", err)
				close(waitc)
				return
			}

			// 3. Translate the Integer back to Logic
			actionCode := response.GetAction()
			actionString := "HOLD"
			if actionCode == 1 {
				actionString = "BUY"
			} else if actionCode == 2 {
				actionString = "SELL"
			}

			t.Logf("SUCCESS: Python analyzed the tick and replied -> %s (Code: %d)", actionString, actionCode)
		}
	}()

	// 4. The Sending Thread (The Pitcher)
	testTick := &routeguide.InputData{
		ID:        1,
		EventType: "trade",
		Price:     "64295.24",
		Quantity:  "0.0093",
	}

	err = stream.Send(testTick)
	if err != nil {
		t.Fatalf("Failed to send test tick: %v", err)
	}
	t.Logf("Sent Tick #1 down the pipe...")

	stream.CloseSend()

	<-waitc
}