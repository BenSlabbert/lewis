package main

import (
	"context"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	protocolpb "lewis/gen/pb/protocol"
	"lewis/pkg/util"
	"lewis/pkg/writer"
	"log"
	"sync"
)

type Server struct {
	ctx context.Context

	credentials    map[string]string
	credentialsMtx sync.Mutex

	writer *writer.Writer
}

func newServer(ctx context.Context, aofPath, idPath string) *Server {
	newWriter, err := writer.NewWriter(aofPath, idPath)
	if err != nil {
		log.Fatalln(err)
	}

	return &Server{ctx: ctx, credentials: make(map[string]string), credentialsMtx: sync.Mutex{}, writer: newWriter}
}

func (s *Server) Read(req *protocolpb.ReadRequest, stream protocolpb.LewisService_ReadServer) error {
	switch t := req.GetReadType().(type) {
	case *protocolpb.ReadRequest_FromId:
		return status.Errorf(codes.Unimplemented, "not implemented")
	case *protocolpb.ReadRequest_Beginning:
		return s.readFromBeginning(stream)
	case *protocolpb.ReadRequest_Latest:
		return s.readLatestMessages(stream)
	default:
		return status.Errorf(codes.InvalidArgument, "unable to handle read type %t", t)
	}
}

func (s *Server) readLatestMessages(stream protocolpb.LewisService_ReadServer) error {
	u := uuid.New()
	messages := s.writer.SubscribeToLatestMessages(u)
	defer s.writer.UnSubscribeToLatestMessages(u)

	for {
		select {
		case <-stream.Context().Done():
			log.Println("client closing")
			return nil
		case message := <-messages:
			err := stream.Send(&protocolpb.ReadResponse{
				Id:    message.Id,
				Value: message.Body,
			})
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) readFromBeginning(stream protocolpb.LewisService_ReadServer) error {
	msgChan, err := s.writer.ReadFromBeginning()
	if err != nil {
		return err
	}

	for message := range msgChan {
		err = stream.Send(&protocolpb.ReadResponse{
			Id:    message.Id,
			Value: message.Body,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) Write(ctx context.Context, req *protocolpb.WriteRequest) (*protocolpb.WriteResponse, error) {
	id, err := s.writer.SyncWrite(req.GetValue())
	if err != nil {
		return nil, err
	}

	return &protocolpb.WriteResponse{
		Id: id,
	}, nil
}

func (s *Server) Init() {
	log.Println("waiting for stop server")

	select {
	case <-s.ctx.Done():
		util.CloseQuietly(s.writer)
		log.Println("stop server")
	}
}

func (s *Server) Login(ctx context.Context, req *protocolpb.LoginRequest) (*protocolpb.LoginResponse, error) {
	s.credentialsMtx.Lock()
	defer s.credentialsMtx.Unlock()

	s.credentials["token"] = "token"

	return &protocolpb.LoginResponse{
		Token: "token",
	}, nil
}

func (s *Server) authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.InvalidArgument, "unable to get credentials from metadata")
	}

	_, ok = md[protocolpb.MD_uname.String()]
	if !ok {
		return status.Error(codes.Unauthenticated, "no name provided")
	}

	token, ok := md[protocolpb.MD_token.String()]
	if !ok {
		return status.Error(codes.Unauthenticated, "no token provided")
	}

	s.credentialsMtx.Lock()
	defer s.credentialsMtx.Unlock()

	_, ok = s.credentials[token[0]]
	if !ok {
		return status.Error(codes.Unauthenticated, "invalid token provided")
	}

	return nil
}

func (s *Server) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.authorize(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *Server) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if info.FullMethod != "/protocol_pb.LewisService/Login" {
		if err := s.authorize(ctx); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}
