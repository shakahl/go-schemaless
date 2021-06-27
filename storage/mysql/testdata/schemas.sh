#!/bin/bash

SQL=cell-shards.sql

set -eux

docker cp $SQL schemaless_mariadb1:/
docker exec schemaless_mariadb1 mariadb \
	--host=localhost \
	--port=13306 \
	--user=sltest \
	--password=`cat $HOME/.mysql_password` \
	trips \
	-e "source /$SQL"

docker cp $SQL schemaless_mariadb2:/
docker exec schemaless_mariadb2 mariadb \
	--host=localhost \
	--port=13307 \
	--user=sltest \
	--password=`cat $HOME/.mysql_password` \
	trips \
	-e "source /$SQL"
