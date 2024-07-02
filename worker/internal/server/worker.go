package server

import (
	"encoding/json"
	"log"
	"mapreduce_worker/internal/dto"
	"mapreduce_worker/internal/mapreduce"
	"mapreduce_worker/internal/utils"
	"os"
	"strconv"
	"time"
)

func Worker(mapf func(string, string) []dto.KeyValue,
	reducef func(string, []string) string,
	mergef func([]string, string),
	id string,
) {
	for {
		idNum, err := strconv.Atoi(id)
		if nil != err {
			log.Fatal(err)
		}
		args := dto.Args{
			TaskNo: idNum,
		}
		reply := dto.Reply{}

		res := utils.Call("Master.GetTask", args, &reply)
		if !res {
			log.Fatal("Failed to perform rpc call")
		}

		a, err := json.Marshal(args)
		if nil != err {
			log.Fatal(err)
		}
		log.Printf("Args: %s\n", string(a))

		r, err := json.Marshal(reply)
		if nil != err {
			log.Fatal(err)
		}
		log.Printf("Reply: %s\n", string(r))

		switch reply.Task.Task {
		case dto.Map:
			mapreduce.DoMap(reply, mapf)
		case dto.Reduce:
			mapreduce.DoReduce(reply, reducef)
		case dto.Merge:
			mapreduce.DoMerge(reply, mergef)
		case dto.Wait:
			time.Sleep(10 * time.Second)
		case dto.Exit:
			log.Print("Exit called")
			os.Exit(0)
		}
	}
}
