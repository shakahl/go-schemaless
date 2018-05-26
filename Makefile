
test:
	go test -v 
	cd models; go test -v
	cd core; go test -v
	cd storage/memory; go test -v
	cd storage/fs; go test -v
	cd storage/mysql; SQLHOST=localhost DB=user0 go test -v
	cd storage/postgres; SQLHOST=localhost DB=user0 go test -v
	#Skipping Rqlite, it's still experimental.
	#cd storage/rqlite; go test -v

testall:
	go test -v 
	cd models; go test -v
	cd core; go test -v
	cd storage/memory; go test -v
	cd storage/fs; go test -v
	cd storage/mysql; SQLHOST=localhost DB=user0 go test -v
	cd storage/postgres; SQLHOST=localhost DB=user0 go test -v
	cd storage/rqlite; go test -v
