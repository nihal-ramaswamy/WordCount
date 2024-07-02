package main

import "mapreduce_master/internal/server"

func main() {
	files := []string{"a.txt", "b.txt", "c.txt"}
	numReducers := 3
	doneCh := make(chan bool, 1)

	server.InitMaster(files, numReducers, doneCh)

	for done := range doneCh {
		if done {
			close(doneCh)
			return
		}
	}
}
