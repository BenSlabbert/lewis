package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	protocolpb "lewis/gen/pb/protocol"
	"lewis/pkg/util"
	"log"
)

// GitCommit is set during compilation
var GitCommit string

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Running server build commit: %s", GitCommit)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", 50052), opts...)
	if err != nil {
		log.Fatalln(err)
	}
	defer util.CloseQuietly(conn)

	client := protocolpb.NewLewisServiceClient(conn)
	login, err := client.Login(context.Background(), &protocolpb.LoginRequest{
		Username: "uname",
		Password: "pswd",
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(login)

	pairs := metadata.Pairs(protocolpb.MD_uname.String(), "client1", protocolpb.MD_token.String(), login.GetToken())
	ctx := metadata.NewOutgoingContext(context.Background(), pairs)

	log.Println("read from beginning start")
	readFromBeginning(ctx, client)
	log.Println("read from beginning end")

	log.Println("read latest start")
	readLatest(ctx, client)
	log.Println("read latest end")
}

func readLatest(ctx context.Context, client protocolpb.LewisServiceClient) {
	go func() {
		for i := 0; i < 10; i++ {
			_, _ = client.Write(ctx, &protocolpb.WriteRequest{
				Value: []byte(fmt.Sprintf("hello world-%d", i)),
			})
		}
	}()

	stream, err := client.Read(ctx, &protocolpb.ReadRequest{
		ReadType: &protocolpb.ReadRequest_Latest{},
	})
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < 10; i++ {
		recv, err := stream.Recv()

		if err != nil {
			log.Fatalln(err)
		}

		id := recv.GetId()
		value := recv.GetValue()
		log.Printf("id: %d message: %s", id, string(value))
	}

	err = stream.CloseSend()
	if err != nil {
		log.Fatalln(err)
	}
}

func readFromBeginning(ctx context.Context, client protocolpb.LewisServiceClient) {
	write, err := client.Write(ctx, &protocolpb.WriteRequest{
		Value: []byte("hello world"),
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(write)

	stream, err := client.Read(ctx, &protocolpb.ReadRequest{
		ReadType: &protocolpb.ReadRequest_Beginning{},
	})
	if err != nil {
		log.Fatalln(err)
	}

	for {
		recv, err := stream.Recv()
		if err == io.EOF {
			log.Println("got end of stream")
			err = stream.CloseSend()
			if err != nil {
				log.Fatalln(err)
			}
			return
		}

		id := recv.GetId()
		value := recv.GetValue()
		log.Printf("id: %d message: %s", id, string(value))
	}
}
