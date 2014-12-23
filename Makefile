default: test

deps:
	go get -t ./...

test: deps
	go test -v ./...

race: deps
	go test -v -race ./...
