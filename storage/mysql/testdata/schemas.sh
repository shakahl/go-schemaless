#!/bin/bash

SQL=cell-shards.sql

docker cp $SQL schemaless_mariadb1:/
docker exec schemaless_mariadb1 mariadb \
	--host=127.0.0.1 \
	--port=3306 \
	--user=sltest \
	--password=`cat /home/elysium/.mysql_password` \
	trips \
	-e "source /$SQL"

docker cp $SQL schemaless_mariadb2:/
docker exec schemaless_mariadb2 mariadb \
	--host=127.0.0.1 \
	--port=3306 \
	--user=sltest \
	--password=`cat /home/elysium/.mysql_password` \
	trips \
	-e "source /$SQL"

docker cp $SQL schemaless_mariadb3:/
docker exec schemaless_mariadb3 mariadb \
	--host=127.0.0.1 \
	--port=3306 \
	--user=sltest \
	--password=`cat /home/elysium/.mysql_password` \
	trips \
	-e "source /$SQL"
