
schemaless-mysql=SQLUSER=$(MYSQLUSER) SQLPASS=$(MYSQLPASS) SQLHOST=localhost
schemaless-postgres=SQLUSER=$(PGUSER) SQLPASS=$(PGPASS) SQLHOST=localhost

test:
	go test -v 
	cd models; go test -v
	cd core; go test -v
	cd storage/memory; go test -v
	cd storage/fs; go test -v
	cd storage/mysql; $(schemaless-mysql) DB=user0 go test -v
	cd storage/postgres; $(schemaless-postgres) DB=user0 go test -v
