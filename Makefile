
test:
	cd storage/sqlite; go test -v

testall:
	go test -v ./...
