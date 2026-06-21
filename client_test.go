package main

import(
	"context"
	"ApexTrade/routeguide"
	"testing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTGRPCBrdige(t *testing.T){
	conn ,err:=grpc.NewClient("localhost:50051",grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err!=nil{
		t.Fatalf("Failed to connect to Python server")
	}
	defer conn.Close()
	client:=routeguide.NewRouteGuideClient(conn)

	stream,err:=client.SendData(context.Background())
	if err!=nil{
		t.Fatalf("Error opening gRPC stream: %v",err)
	}
	testTick:=&routeguide.InputData{
		ID:1,
		EventType:"trade",
		Price:"64295.5",
		Quantity:"0.0093",
	}
	err=stream.Send(testTick)
	if err!=nil{
		t.Fatalf("Failed to send tick:%v",err)
	}
	t.Logf("Test tick successfully  fired into the gRPC pipe!")
}