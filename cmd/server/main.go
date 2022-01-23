package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	protocolpb "lewis/gen/pb/protocol"
	"lewis/pkg/util"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// GitCommit is set during compilation
var GitCommit string

var aofPath string
var idPath string

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Running server build commit: %s", GitCommit)

	parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newServer(ctx, aofPath, idPath)
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

func parseFlags() {
	aofPathPtr := flag.String("aof", "", "path to append only file")
	idPathPtr := flag.String("id", "", "path to id file")
	flag.Parse()

	if *aofPathPtr == "" {
		path, err := ioutil.TempFile("", "append_only_file")
		if err != nil {
			log.Fatalln(err)
		}
		util.CloseQuietly(path)
		aofPath = path.Name()
	}

	if *idPathPtr == "" {
		path, err := ioutil.TempFile("", "id_file")
		if err != nil {
			log.Fatalln(err)
		}
		util.CloseQuietly(path)
		idPath = path.Name()
	}
}
