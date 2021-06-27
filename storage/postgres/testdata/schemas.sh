#!/bin/bash

SQL=cell-shards.sql

set -eux

psql -U postgres -h 127.0.0.1 -p 15432 -f cell-shards.sql
psql -U postgres -h 127.0.0.1 -p 15433 -f cell-shards.sql

