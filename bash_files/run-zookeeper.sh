#!/bin/bash

cd 'C:/kafka'

printf "init zookeeper"

./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
command &

