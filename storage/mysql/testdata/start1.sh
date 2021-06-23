#!/bin/bash

set -eux

docker run -d \
	-p 13306:3306 \
	--name schemaless_mariadb1 \
	--env MYSQL_DATABASE=trips \
	--env MYSQL_USER=sltest \
	--env MYSQL_PASSWORD=`cat $HOME/.mysql_password` \
	--env MYSQL_ROOT_PASSWORD=`cat $HOME/.mysql_root_password` \
	--rm \
	mariadb:10.5

