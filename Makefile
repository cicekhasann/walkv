.PHONY: build test race bench clean run

BINARY := bin/walkv

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/walkv

test:
	go test -v ./pkg/...

race:
	go test -v -race ./pkg/...

bench:
	go test -run=^$$ -bench=. -benchmem ./pkg/...

clean:
	rm -rf bin/ data/
