
This is an open-source, MIT-licensed implementation of Uber's Schemaless
written in Golang. That makes it's a sharded MySQL database framework. I also
support Postgres (for Postgres fans) and SQLite (for learning purposes)

If you are not familiar with what that means, please see the list of resources
at the end of this README. It will explain everything much better than I can
attempt to do so here. The differences between my implementation and Uber's (as
they have spoken or written about it) are documented throughout the code and in
the github issues.

INSPIRATION

I have been inspired by my other work on github.com/rbastic/dyndao, my time
implementing JSON-powered relational database solutions, and my time spent
sharding large MySQL database clusters during my past life @ Booking.com.

Unfortunately ... it has been years now without Uber open-sourcing their
Schemaless implementation ... but the possibility for open-sourcing was
mentioned in one of their talks over *three* years ago.

I finally decided...  Carpe diem! If they release their implementation, it
will be even better for the open source community. With two competing
implementations available, 'hybrid vigor' becomes possible!

That all aside, I would like to extend my invitation to others to join in and
hack on this project, with a sort of "Linus and the Bazaar" model being
possible.  However, I promise I will be very polite to everyone :-) The Go
community code of conduct rules apply.

CHOOSING A DATABASE

Are you more comfortable with SQLite, MySQL or Postgres? For first-timers
interested in learning how this all works, I recommend starting with SQLite.
That way, you don't have to install anything.

Otherwise, I would say pick MySQL. Sorry, advanced Postgres users. No hard
feelings though, which is why I added a Postgres storage layer :-)

ADDING SUPPORT FOR ADDITIONAL DATABASES / STORAGES

If you wish to add support for additional storages, please file issues/submit
PRs. I will be more than happy to accept well-tested, high-quality
implementations for other potential storage backends. If you need support for
something (but don't see it here) then please file an issue to open discussion
up.

SETTING UP FOR DEVELOPMENT AND RUNNING TESTS

You will have to install and configure any databases that you're interested in
using this framework with. Except for SQLite, hopefully :-) The current RDBMS
options are MySQL and Postgres.

Then you should investigate tools/create_shard_schemas.  This tool is used for
generating schema creation files to play with (warning: they also drop
schemas). It is currently aimed at those wishing to learn about sharding, and
also to facilitate testing on my end -- so it only generates SQL for a single
database master.

(TODO: Future versions will split the tool's functionality in a way
that it can be integrated into an organization's "data fabric", creating
schemas + grants semi-automatically for you, on different sets of shards)

Once you've configured the database with some schemas, I like adding aliases:

MYSQLUSER=....
MYSQLPASS=....

PGUSER=....
PGPASS=...

You'll have to create the 'user' schemas - that's what the Makefile likes.
Or just point it at your own preference.

Now, you can run tests a bit more easily. For me, this looks like:

~/go-src/src/github.com/rbastic/go-schemaless$ make test

Investigate and edit the Makefile if needed.

Any test cases should be designed in a way that they are repeatable. They
should not result in errors on subsequent runs, due to the immutable,
append-only nature of Schemaless.

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

The underlying sharding code is github.com/dgryski/go-shardedkv/choosers,
similar versions of which have powered github.com/photosrv/photosrv and also
sharded MySQL databases. The storage and storagetest code is also derived from
github.com/dgryski/go-shardedkv.

My sincere thanks to Damian Gryski for open-sourcing the above package.

OTHER RESOURCES

FriendFeed: https://backchannel.org/blog/friendfeed-schemaless-mysql

Pinterest: https://engineering.pinterest.com/blog/sharding-pinterest-how-we-scaled-our-mysql-fleet

Martin Fowler's slides on Schemaless Data Structures: https://martinfowler.com/articles/schemaless/

OTHER OPEN-SOURCE IMPLEMENTATIONS

https://github.com/hoteltonight/shameless - Schemaless in Ruby

