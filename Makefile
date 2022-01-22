.PHONY: build fmt vet mod upx docker proto-gen clean

GIT_COMMIT_ID = $(shell git log --format="%H" -n 1)
GIT_BRANCH_NAME = $(shell git symbolic-ref --short -q HEAD)

build: mod fmt vet proto-gen test
	GOOS=linux ARCH=amd64 CGO_ENABLED=0 go build -tags netgo -a -v -ldflags "-s -w -X main.GitCommit=$(GIT_COMMIT_ID)" -o bin/cmd/client cmd/client/*.go
	GOOS=linux ARCH=amd64 CGO_ENABLED=0 go build -tags netgo -a -v -ldflags "-s -w -X main.GitCommit=$(GIT_COMMIT_ID)" -o bin/cmd/server cmd/server/*.go

# https://upx.github.io/
upx:
	upx -o bin/cmd/server.upx bin/cmd/server
	upx -o bin/cmd/client.upx bin/cmd/client

docker: build upx
	docker build --build-arg BIN_NAME=server -f docker/Dockerfile . -t lewis-server -t lewis-server:$(GIT_COMMIT_ID) -t lewis-server:$(GIT_BRANCH_NAME)
	docker build --build-arg BIN_NAME=client -f docker/Dockerfile . -t lewis-client -t lewis-client:$(GIT_COMMIT_ID) -t lewis-client:$(GIT_BRANCH_NAME)
	docker build --build-arg BIN_NAME=server.upx -f docker/Dockerfile . -t lewis-server-upx -t lewis-server-upx:$(GIT_COMMIT_ID) -t lewis-server-upx:$(GIT_BRANCH_NAME)
	docker build --build-arg BIN_NAME=client.upx -f docker/Dockerfile . -t lewis-client-upx -t lewis-client-upx:$(GIT_COMMIT_ID) -t lewis-client-upx:$(GIT_BRANCH_NAME)

fmt:
	go fmt ./...

vet:
	go vet ./...

test:
	go test ./...

mod:
	go mod tidy
	go mod verify

proto-gen:
	protoc --go_out=gen/pb/protocol --go-grpc_out=gen/pb/protocol --go-grpc_opt=require_unimplemented_servers=false pkg/protocol/protocol.proto

clean:
	rm -rf bin
	rm -rf *.bin
