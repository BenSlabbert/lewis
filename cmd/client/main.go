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
	"time"
)

// GitCommit is set during compilation
var GitCommit string

var count = 0

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
			write(ctx, client)
		}
	}()

	stream, err := client.Read(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.Send(&protocolpb.ReadRequest{
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
	for i := 0; i < 20; i++ {
		write(ctx, client)
	}

	stream, err := client.Read(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.Send(&protocolpb.ReadRequest{
		ReadType: &protocolpb.ReadRequest_Beginning{},
	})
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		for {
			<-time.After(250 * time.Millisecond)
			write(ctx, client)
		}
	}()

	lastId := uint64(0)

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

		// take time to do work
		<-time.After(50 * time.Millisecond)

		id := recv.GetId()
		value := recv.GetValue()
		log.Printf("received id: %d message: %s", id, string(value))

		if id-lastId != 1 {
			log.Fatalf("last id received was %d current id is %d, error, ids are not in sequence", lastId, id)
		}
		lastId = id

		err = stream.Send(&protocolpb.ReadRequest{
			ReadType: &protocolpb.ReadRequest_AckId{
				AckId: id,
			},
		})
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func write(ctx context.Context, client protocolpb.LewisServiceClient) {
	count++
	var err error
	_, err = client.Write(ctx, &protocolpb.WriteRequest{
		Value: []byte(fmt.Sprintf("hello world - %d", count)),
	})
	if err != nil {
		log.Fatalln(err)
	}
}
