#!/bin/bash

# Set the environment variables
build_dir="../build"
server="$build_dir/bin/easydb_server"
client="$build_dir/bin/easydb_client"
port="8766" # 8765 / 8790
database="test_db"
database_path="$build_dir/$database"

# test
test_sql="test.sql"
$client -p $port < $test_sql > $test_sql.log 2>&1
