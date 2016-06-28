HUMMINGBIRD_VERSION?=$(shell git describe --tags)

all: bin/hummingbird

bin/hummingbird: */*.go
	mkdir -p bin
	go build -o bin/hummingbird -ldflags "-X main.Version=$(HUMMINGBIRD_VERSION)" cmd/hummingbird/main.go

get:
	go get -t ./...

fmt:
	go fmt ./...

test:
	@test -z "$(shell find . -name '*.go' | xargs gofmt -l)" || (echo "You need to run 'go fmt ./...'"; exit 1)
	go vet ./...
	go test -cover ./...

install: bin/hummingbird
	cp bin/hummingbird $(DESTDIR)/usr/bin/hummingbird

develop: bin/hummingbird
	ln -f -s bin/hummingbird /usr/local/bin/hummingbird

