This example initializes 4 shards (using the SQLite-backed fs storage, for
testing purposes) and writes 1000 fake user records (data generated using
icrowley/fake),

Records are sharded to different SQLite database files using Jump Hash +
metrohash. You can imagine here that each file is an actual shard, or cluster
of SQL databases. The UUIDs are generated using satori's go.uuid package.

When you run 'go run main.go', you should see four user db files appear in the
current folder. You can observe the following:

rbastic@marvin:~/go-src/src/github.com/rbastic/go-schemaless/examples/sqlite_users$  go run main.go
(output snipped...)

{"level":"info","ts":1526624266.7416005,"caller":"fs/fs.go:241","msg":"ID = 281, affected = 1\n"}
{"level":"info","ts":1526624266.7613523,"caller":"fs/fs.go:241","msg":"ID = 231, affected = 1\n"}
{"level":"info","ts":1526624266.7874675,"caller":"fs/fs.go:241","msg":"ID = 246, affected = 1\n"}

rbastic@marvin:~/go-src/src/github.com/rbastic/go-schemaless/examples/sqlite_users$ ls
main.go  README.md  user0_cell.db  user1_cell.db  user2_cell.db  user3_cell.db

rbastic@marvin:~/go-src/src/github.com/rbastic/go-schemaless/examples/sqlite_users$ sqlite3 user0_cell.db 
SQLite version 3.11.0 2016-02-15 17:29:24
Enter ".help" for usage hints.

sqlite> select * from cell limit 10;
1|8628d071-3bc7-4946-abd8-595def591c8e|PII|7|{"name": "Jimmy Cox"}|2018-05-18 06:17:24
2|dacd5e40-25d3-4185-a810-845dc8b95797|PII|9|{"name": "Charles Fernandez"}|2018-05-18 06:17:24
3|7889369f-8d26-48de-8af0-fdb52146b070|PII|12|{"name": "Heather Hernandez"}|2018-05-18 06:17:24
4|2dcbe4fb-b28b-4a10-ae96-d8db69f3521c|PII|13|{"name": "Evelyn Wells"}|2018-05-18 06:17:24
5|aea4e735-2f35-4cb3-9f63-4db6de8fd8d6|PII|15|{"name": "Susan Morales"}|2018-05-18 06:17:24
6|71eb11dd-ed5d-46c7-bf3f-2a3f72d84971|PII|16|{"name": "Louis Ross"}|2018-05-18 06:17:24
7|392c62fd-7d15-4f05-a622-4da6118397ef|PII|30|{"name": "Benjamin Williamson"}|2018-05-18 06:17:24
8|ffe1905e-3e3d-49a0-b457-906c9bd378b6|PII|34|{"name": "Joe Barnes"}|2018-05-18 06:17:24
9|ab729d55-9222-4f22-9b01-cc84168c3775|PII|38|{"name": "Jeremy Porter"}|2018-05-18 06:17:25
10|fdde186d-376c-486e-912d-4d675f054d16|PII|42|{"name": "Kimberly Phillips"}|2018-05-18 06:17:25

The records are sharded among the "shards" on the second column above -- the row key.
