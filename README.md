This is an open-source, MIT-licensed implementation of Uber's Schemaless
(immutable BigTable-style sharded MySQL/Postgres). Consider this as a potential
building block for your own sharded data storage APIs and infrastructure.

The github issues list describes what has been intentionally left unimplemented and
what differences there are between this implementation and Uber's (based on the materials
linked at the end.)

All code is in Go.

API SUPPORTED

```
GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error)

GetCellLatest(ctx context.Context, rowKey string, columnKey string) (cell models.Cell, found bool, err error)

PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error)

PutCell(ctx context.Context, rowKey string, columnKey string, refKey int64, cell models.Cell) (err error)

ResetConnection(ctx context.Context, key string) error

Destroy(ctx context.Context) error
```

DATABASE SUPPORT

For learning or other:

	* SQLite

	* Badger (experimental, local storage only)

For more serious testing and usage:

	* MySQL

	* Postgres

SETTING UP FOR DEVELOPMENT AND RUNNING TESTS

1. Install MySQL and Postgres

2. Setup users on MySQL and Postgres

3. Run both shell scripts inside tools/create_shard_schemas, one at a time,
loading the generated sql file into Postgres and MySQL locally.

4. Now, you can run tests a bit more easily. For me, this looks like:

~/go-src/src/github.com/rbastic/go-schemaless$ MYSQLUSER=user MYSQLPASS=pass PGUSER=user PGPASS=pass SQLHOST=localhost make test

Having replaced the user and pass with the appropriate usernames and passwords
for MySQL and Postgres, this should pass all tests.

Any test cases should be idempotent - they should not result in errors on
subsequent runs, for example, due to hard-coded row keys.

DISCLAIMER

I do not work for Uber Technologies.

VIDEOS

"Taking Storage for a Ride With Uber", https://www.youtube.com/watch?v=Dg76cNaeB4s (30 mins)

"GOTO 2016 • Taking Storage for a Ride", https://www.youtube.com/watch?v=kq4gp90QUcs (1 hour)

ARTICLES

"Designing Schemaless, Uber Engineering’s Scalable Datastore Using MySQL"

"Part One", https://eng.uber.com/schemaless-part-one/

"Part Two", https://eng.uber.com/schemaless-part-two/

"Part Three", https://eng.uber.com/schemaless-part-three/

"Code Migration in Production: Rewriting the Sharding Layer of Uber’s Schemaless Datastore"
https://eng.uber.com/schemaless-rewrite/

Most of the underlying implementation is derived from https://github.com/dgryski/go-shardedkv.

OTHER RESOURCES

FriendFeed: https://backchannel.org/blog/friendfeed-schemaless-mysql

Pinterest: https://engineering.pinterest.com/blog/sharding-pinterest-how-we-scaled-our-mysql-fleet

Martin Fowler's slides on Schemaless Data Structures: https://martinfowler.com/articles/schemaless/

OTHER OPEN-SOURCE IMPLEMENTATIONS

https://github.com/hoteltonight/shameless - Schemaless in Ruby

THANKS

To Damian Gryski for releasing https://github.com/dgryski/go-shardedkv.

To Booking.com for allowing the release of https://github.com/photosrv/photosrv.

To Uber Technologies for releasing numerous materials on the design and
implementation of Mezzanine, their Schemaless store.

And to many others :)
