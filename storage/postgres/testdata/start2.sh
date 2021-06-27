#!/bin/bash

set -eux

docker run --name schemaless_pgdb2 -p 15433:5432 --network default -e POSTGRES_PASSWORD=`cat $HOME/.postgres_password` -d postgres
