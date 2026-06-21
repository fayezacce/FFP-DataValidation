#!/bin/sh
export PGPASSWORD='fayez_secret'
psql -U fayez -d ffp_validator -Atc "select column_name from information_schema.columns where table_name='valid_records' order by ordinal_position;"
