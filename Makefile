.PHONY: build fmt vet mod

build: mod fmt vet proto-gen
	GOOS=linux go build -ldflags "-s -w" -o bin/cmd/client cmd/client/*.go
	GOOS=linux go build -ldflags "-s -w" -o bin/cmd/server cmd/server/*.go

# https://upx.github.io/
upx:
	upx -o bin/cmd/server.upx bin/cmd/server
	upx -o bin/cmd/client.upx bin/cmd/client

fmt:
	go fmt ./...

vet:
	go vet ./...

mod:
	go mod tidy
	go mod verify

proto-gen:
	protoc --go_out=gen/pb/protocol --go-grpc_out=gen/pb/protocol --go-grpc_opt=require_unimplemented_servers=false pkg/protocol/protocol.proto

clean:
	rm -rf bin
	rm -rf *.bin
