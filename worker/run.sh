#!/bin/sh

go run main.go 1 &
P1=$!
go run main.go 2 &
P2=$!
go run main.go 3 &
P3=$!
wait $P1 $P2 $P3
