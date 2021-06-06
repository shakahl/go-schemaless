This is an open-source, MIT-licensed implementation of Uber's Schemaless
(immutable BigTable-style sharded MySQL/Postgres). Consider this as a potential
learning grounds for your own scalable data storage APIs and infrastructure.

This is only a pet project and not production ready in any capacity.

The github issues list describes what has been intentionally left unimplemented and
what differences there are between this implementation and Uber's (based on the materials
linked at the end.)

All code is in Go.

API SUPPORTED

```
Get(ctx context.Context, tableName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error)

GetLatest(ctx context.Context, tableName, rowKey, columnKey string) (cell models.Cell, found bool, err error)

PartitionRead(ctx context.Context, tableName string, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error)

Put(ctx context.Context, tableName, rowKey, columnKey string, refKey int64, jsonBody string) (err error)

ResetConnection(ctx context.Context, key string) error

Destroy(ctx context.Context) error
```

DATABASE SUPPORT

For learning or other:

	* SQLite

For more serious testing and usage:

	* MySQL

	* Postgres

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

SIMILAR OPEN-SOURCE WORK

https://github.com/hoteltonight/shameless - A similar append-only data store in Ruby influenced by Schemaless

THANKS

To Damian Gryski for releasing https://github.com/dgryski/go-shardedkv. This code is significantly derived from Damian's excellent work.

To Uber Technologies for releasing numerous materials on the design and implementation of Mezzanine, their Schemaless store.

And to many others :)
