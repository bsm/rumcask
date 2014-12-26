default: test

deps:
	go get -t ./...

test: deps
	go test ./...

race: deps
	go test -race ./...

bench: deps
	go test -test.run=NONE -test.bench=.
