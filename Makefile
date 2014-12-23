default: test

deps:
	go get -t ./...

test: deps
	go test -v ./...

race: deps
	go test -v -race ./...

bench: deps
	go test -test.run=NONE -test.bench=.
