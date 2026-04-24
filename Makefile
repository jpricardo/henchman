protoc:
	protoc --go_out=gen --go_opt=paths=source_relative --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/henchman/v1/cache.proto

build:
	go build ./cmd/henchd

run:
	go run ./cmd/henchd

test:
	go test ./...

buf:
	buf generate
