This example initializes 4 shards (using the Postgres-backed storage, for testing
purposes) and writes 1000 fake user records (data generated using
icrowley/fake). The example provided does not yet connect to different
databases, it assumes all schemas live on a single host.

You will have to have created the shard schemas locally using
tools/create_shard_schemas.

Records are sharded to the different schemas using Jump Hash + metrohash. You
can imagine here that each schema represents an actual shard-cluster. The UUIDs
are generated using satori's go.uuid package.

When you run 'go run main.go', you should see four user db files appear in the
current folder. You can observe the following:

rbastic@marvin:~/go-src/src/github.com/rbastic/go-schemaless/examples/postgres_users$

SQLUSER=yourUser SQLPASS=yourPass DB=yourDB SQLHOST=localhost go run main.go (output snipped...)

{"level":"info","ts":1526624266.7416005,"caller":"fs/fs.go:241","msg":"ID = 281, affected = 1\n"}
{"level":"info","ts":1526624266.7613523,"caller":"fs/fs.go:241","msg":"ID = 231, affected = 1\n"}
{"level":"info","ts":1526624266.7874675,"caller":"fs/fs.go:241","msg":"ID = 246, affected = 1\n"}

Then go check your postgres installation, maybe using something like:

SELECT * FROM user0.cell UNION ALL 
SELECT * FROM user1.cell UNION ALL 
SELECT * FROM user2.cell UNION ALL 
SELECT * FROM user3.cell;

You should all of the fake user data.
