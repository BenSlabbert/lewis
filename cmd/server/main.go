package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	protocolpb "lewis/gen/pb/protocol"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var GitCommit string

func main() {
	log.Printf("Running server build commit: %s", GitCommit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newServer(ctx)
	go s.Init()

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(s.UnaryInterceptor), grpc.StreamInterceptor(s.StreamInterceptor))
	protocolpb.RegisterLewisServiceServer(grpcServer, s)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 50052))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// https://gist.github.com/embano1/e0bf49d24f1cdd07cffad93097c04f0a
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		sig := <-signalChan
		log.Printf("got signal %v, attempting graceful shutdown", sig)
		cancel()
		grpcServer.GracefulStop()
		wg.Done()
	}()

	log.Println("starting grpc server")
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	wg.Wait()

	<-time.After(100 * time.Millisecond)
	log.Println("clean shutdown")
}
