This is an open-source, MIT-licensed implementation of Uber's Schemaless
(immutable BigTable-style sharded MySQL datastore)

All code is in Golang, no exceptions.

DATABASE SUPPORT

For learning or other:

	* SQLite (the 'fs' and 'memory' storages are just file and memory
	  SQLite backends)

	* rqlite (Distributed SQLite) - experimental, broken

For potentially serious usage:

	* MySQL

	* Postgres


ADDING SUPPORT FOR ADDITIONAL DATABASES / STORAGES

I will be more than happy to accept well-tested, high-quality implementations
for other potential storage backends. If you need support for something (but
don't see it here) then please file an issue to open discussion up. PRs
welcome.

SETTING UP FOR DEVELOPMENT AND RUNNING TESTS

1. Install MySQL, postgres, and rqlite, setup users on MySQL and Postgres.

2 Run both shell scripts inside tools/create_shard_schemas, one at a time,
loading the generated sql file into Postgres and MySQL locally.

(TODO: Future versions will split the tool's functionality in a way
that it can be integrated into an organization's "data fabric", creating
schemas + grants semi-automatically for you, on different sets of shards)

3. Now, you can run tests a bit more easily. For me, this looks like:

~/go-src/src/github.com/rbastic/go-schemaless$ MYSQLUSER=user MYSQLPASS=pass PGUSER=user PGPASS=pass SQLHOST=localhost make test

Having replaced the user and pass with the appropriate usernames and passwords
for MySQL and Postgres, this should pass all tests.

Any test cases should be idempotent - they should not result in errors on
subsequent runs due to hard-coded row keys.

DISCLAIMER

I do not work for Uber Technologies. Everything has been sourced from their
materials that they've released on the subject matter (which I am extremely
gracious for): 

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

The underlying sharding code is https://github.com/dgryski/go-shardedkv/choosers,
similar versions of which have powered https://github.com/photosrv/photosrv and
also a large sharded MySQL database system. The storage and storagetest code is
also derived from https://github.com/dgryski/go-shardedkv

My sincere thanks to Damian Gryski for open-sourcing the above package.

OTHER RESOURCES

FriendFeed: https://backchannel.org/blog/friendfeed-schemaless-mysql

Pinterest: https://engineering.pinterest.com/blog/sharding-pinterest-how-we-scaled-our-mysql-fleet

Martin Fowler's slides on Schemaless Data Structures: https://martinfowler.com/articles/schemaless/

OTHER OPEN-SOURCE IMPLEMENTATIONS

https://github.com/hoteltonight/shameless - Schemaless in Ruby

