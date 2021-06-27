#!/bin/bash

set -eux

docker run --name schemaless_pgdb1 -p 15432:5432 --network default -e POSTGRES_PASSWORD=`cat $HOME/.postgres_password` -d postgres


