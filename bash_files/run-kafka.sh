#!/bin/bash

cd 'C:/kafka'

printf "init kafka"

./bin/windows/kafka-server-start.bat ./config/server.properties

