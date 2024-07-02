package main

import (
	"mapreduce_worker/internal/mapreduce"
	"mapreduce_worker/internal/server"
	"os"
)

func main() {
	id := os.Args[1]
	server.Worker(mapreduce.Map, mapreduce.Reduce, mapreduce.Merge, id)
}
