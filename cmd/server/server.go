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
	writer         *writer.Writer
}

func newServer(ctx context.Context, aofPath, idPath string) *Server {
	newWriter, err := writer.NewWriter(aofPath, idPath)
	if err != nil {
		log.Fatalln(err)
	}

	return &Server{ctx: ctx, credentials: make(map[string]string), credentialsMtx: sync.Mutex{}, writer: newWriter}
}

type messageQueue struct {
	*util.Queue
}

func newMessageQueue(maxLength int) *messageQueue {
	return &messageQueue{
		Queue: util.NewQueue(maxLength),
	}
}

func (mq *messageQueue) Push(payload *writer.Message) {
	mq.Queue.Push(payload)
}

func (mq *messageQueue) Peek() *writer.Message {
	peek := mq.Queue.Peek()

	if peek == nil {
		return nil
	}

	return peek.(*writer.Message)
}

func (mq *messageQueue) Poll() *writer.Message {
	poll := mq.Queue.Poll()

	if poll == nil {
		return nil
	}

	return poll.(*writer.Message)
}

func (s *Server) Read(stream protocolpb.LewisService_ReadServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	switch t := req.GetReadType().(type) {
	case *protocolpb.ReadRequest_FromId:
		// determine if this id is in the cache or not
		// if it is in the cache serve the response from the cache and subscribe for new messages
		// when possible to just serve from the new message subscription

		// if not in the cache, read from file
		// read from file until the file read has caught up to the cache, then switch
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
	lastMessageSent, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err := s.readFromAofUntilOverlap(stream)
	if err != nil {
		return err
	}
	defer s.writer.UnSubscribeToLatestMessages(subscriptionUUID)

	log.Printf("lastMessageSent: %d", lastMessageSent.Id)
	log.Printf("latestMessagesQueue.Length: %v", latestMessagesQueue.Length())
	log.Printf("latestMessagesQueue.Peek: %d", latestMessagesQueue.Peek().Id)

	// pop messages off the queue until we get the next message to send
	for {
		peek := latestMessagesQueue.Peek()
		if peek == nil {
			break
		}

		if peek.Id > lastMessageSent.Id {
			break
		}

		msg := latestMessagesQueue.Poll()
		log.Printf("pop message %d from queue", msg.Id)
	}

	select {
	case message := <-latestMessagesChan:
		log.Printf("received new message, saving to queue %d", message.Id)
		latestMessagesQueue.Push(message)
	default:
	}

loop:
	for {
		select {
		case <-stream.Context().Done():
			log.Println("client closing connection")
			return stream.Context().Err()
		case message := <-latestMessagesChan:
			log.Printf("received new message, saving to queue %d", message.Id)
			latestMessagesQueue.Push(message)
		default:
			log.Printf("latestMessagesQueue.Length %d", latestMessagesQueue.Length())
			msg := latestMessagesQueue.Poll()

			if msg == nil {
				// switch to live stream
				log.Println("message queue finished, switch to live stream")
				break loop
			}
			log.Printf("sending message from queue %d", msg.Id)

			err = stream.Send(&protocolpb.ReadResponse{
				Id:    msg.Id,
				Value: msg.Body,
			})
			if err != nil {
				return err
			}

			// wait for client to ack this message
			var m *protocolpb.ReadRequest
			m, err = stream.Recv()
			if err != nil {
				return err
			}

			log.Printf("client acked message id %v", m)
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			log.Println("client closing connection")
			return stream.Context().Err()
		case msg := <-latestMessagesChan:
			log.Printf("%d: sending message from live stream", msg.Id)
			err = stream.Send(&protocolpb.ReadResponse{
				Id:    msg.Id,
				Value: msg.Body,
			})
			if err != nil {
				return err
			}

			// wait for client to ack this message
			var m *protocolpb.ReadRequest
			m, err = stream.Recv()
			if err != nil {
				return err
			}

			log.Printf("client acked message id %v", m)
		}
	}
}

func (s *Server) readFromAofUntilOverlap(stream protocolpb.LewisService_ReadServer) (*writer.ReadFromBeginningMessage, *messageQueue, uuid.UUID, <-chan *writer.Message, error) {
	var (
		err                      error
		switchToLiveStream       bool
		subscriptionUUID         uuid.UUID
		aofReadCtx               context.Context
		cancelFunc               context.CancelFunc
		aofMessageChan           <-chan *writer.ReadFromBeginningMessage
		latestMessagesChan       <-chan *writer.Message
		latestMessagesQueue      *messageQueue
		message                  *writer.Message
		peek                     *writer.Message
		readFromBeginningMessage *writer.ReadFromBeginningMessage
		lastMessageSent          *writer.ReadFromBeginningMessage
	)

	subscriptionUUID = uuid.New()
	log.Printf("subscription uuid %s", subscriptionUUID.String())

	latestMessagesQueue = newMessageQueue(1000)
	latestMessagesChan = s.writer.SubscribeToLatestMessages(subscriptionUUID)
	defer func() {
		if err != nil {
			s.writer.UnSubscribeToLatestMessages(subscriptionUUID)
		}
	}()

	aofReadCtx, cancelFunc = context.WithCancel(stream.Context())
	defer cancelFunc()

	// this method is creating a lock which is breaking everything when we do not exit cleanly
	aofMessageChan, err = s.writer.ReadFromBeginning(aofReadCtx)
	if err != nil {
		return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err
	}

	for {
		select {
		case <-stream.Context().Done():
			log.Println("client cancelling request")
			err = stream.Context().Err()
			return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err
		case message = <-latestMessagesChan:
			log.Printf("received new message, saving to queue %d", message.Id)
			latestMessagesQueue.Push(message)
		case readFromBeginningMessage = <-aofMessageChan:

			if readFromBeginningMessage == nil {
				// no more messages in the aof, switch to live stream
				log.Println("switchToLiveStream - no new messages from aof")
				return lastMessageSent, latestMessagesQueue, subscriptionUUID, latestMessagesChan, nil
			}

			lastMessageSent = readFromBeginningMessage

			if readFromBeginningMessage.Err != nil {
				err = readFromBeginningMessage.Err
				return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err
			}

			// at some point we need to switch over to the new message stream
			// wait until we are 100 messages away from the live stream
			peek = latestMessagesQueue.Peek()

			if peek != nil {
				log.Printf("peek.Id: %d", peek.Id)
				log.Printf("readFromBeginningMessage.Id: %d", readFromBeginningMessage.Id)
				log.Printf("readFromBeginningMessage.Id > peek.Id %t", readFromBeginningMessage.Id > peek.Id)
				log.Printf("readFromBeginningMessage.Id-peek.Id = %d", readFromBeginningMessage.Id-peek.Id)
			}

			if peek != nil && (readFromBeginningMessage.Id > peek.Id) && (readFromBeginningMessage.Id-peek.Id > 10) {
				log.Println("message queue sufficient overlap with aof")
				switchToLiveStream = true
			}

			log.Printf("%d: sending message from aof", readFromBeginningMessage.Id)
			// send is writing as fast as it can, filling the tcp buffer
			// we need to add an ACK to each message
			err = stream.Send(&protocolpb.ReadResponse{
				Id:    readFromBeginningMessage.Id,
				Value: readFromBeginningMessage.Body,
			})
			if err != nil {
				return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err
			}

			// wait for client to ack this message
			var m *protocolpb.ReadRequest
			m, err = stream.Recv()
			if err != nil {
				return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, err
			}

			log.Printf("client acked message id %v", m)

			if switchToLiveStream {
				log.Println("switchToLiveStream/queue")
				return readFromBeginningMessage, latestMessagesQueue, subscriptionUUID, latestMessagesChan, nil
			}
		}
	}
}

func (s *Server) Write(ctx context.Context, req *protocolpb.WriteRequest) (*protocolpb.WriteResponse, error) {
	log.Println("writing message")
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
