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

var GitCommit string

func main() {
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

	pairs := metadata.Pairs("token", login.GetToken())
	ctx := metadata.NewOutgoingContext(context.Background(), pairs)

	write, err := client.Write(ctx, &protocolpb.WriteRequest{
		Value: []byte("hello world"),
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(write)

	// read back from server
	stream, err := client.Read(ctx, &protocolpb.ReadRequest{
		ReadType: &protocolpb.ReadRequest_FromId{
			FromId: 1,
		},
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
