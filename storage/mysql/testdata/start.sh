#!/bin/bash

set -eux

docker run -d \
	-p 127.0.0.1:13306:3306 \
	--name schemaless_mariadb1 \
	--env MYSQL_DATABASE=trips \
	--env MYSQL_USER=sltest \
	--env MYSQL_PASSWORD=`cat /home/elysium/.mysql_password` \
	--env MYSQL_ROOT_PASSWORD=`cat /home/elysium/.mysql_root_password` \
	--rm \
	mariadb:10.5

docker run -d \
	-p 127.0.0.1:13307:3307 \
	--name schemaless_mariadb2 \
	--env MYSQL_DATABASE=trips \
	--env MYSQL_USER=sltest \
	--env MYSQL_PASSWORD=`cat /home/elysium/.mysql_password` \
	--env MYSQL_ROOT_PASSWORD=`cat /home/elysium/.mysql_root_password` \
	--rm \
	mariadb:10.5

docker run -d \
	-p 127.0.0.1:13308:3308 \
	--name schemaless_mariadb3 \
	--env MYSQL_DATABASE=trips \
	--env MYSQL_USER=sltest \
	--env MYSQL_PASSWORD=`cat /home/elysium/.mysql_password` \
	--env MYSQL_ROOT_PASSWORD=`cat /home/elysium/.mysql_root_password` \
	--rm \
	mariadb:10.5

