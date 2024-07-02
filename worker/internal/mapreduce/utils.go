package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"mapreduce_worker/internal/dto"
	"mapreduce_worker/internal/utils"
	"os"
	"sort"
)

func DoMap(reply dto.Reply, mapf func(string, string) []dto.KeyValue) {
	if dto.InProgress != reply.Task.Status || dto.Map != reply.Task.Task {
		return
	}

	fileBytes, err := os.ReadFile(reply.Task.InputFiles[0])
	if nil != err {
		log.Fatal(err)
	}
	fileString := string(fileBytes)
	kva := mapf(reply.Task.InputFiles[0], fileString)

	numOutputFiles := len(reply.Task.OutputFiles)

	intermediate := make([][]dto.KeyValue, numOutputFiles)
	for _, kv := range kva {
		r := utils.Ihash(kv.Key) % numOutputFiles
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		oname := reply.Task.OutputFiles[r]
		ofile, _ := os.CreateTemp("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	// Update server state of the task, by calling the RPC NotifyComplete
	reply.Task.Status = dto.Done
	replyEx := dto.Reply{}
	utils.CallNotifyTaskDone("Master.NotifyTaskDone", reply, replyEx)
}

func DoReduce(reply dto.Reply, reducef func(string, []string) string) {
	// Load intermediate files
	intermediate := []dto.KeyValue{}
	for m := 0; m < len(reply.Task.InputFiles); m++ {
		file, err := os.Open(reply.Task.InputFiles[m])
		if err != nil {
			log.Fatalf("cannot open %v", reply.Task.InputFiles[m])
		}
		dec := json.NewDecoder(file)
		for {
			var kv dto.KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort intermediate key-value pairs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create output file
	oname := reply.Task.OutputFiles[0]
	ofile, _ := os.CreateTemp("", oname)

	// Apply reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// Close output file
	ofile.Close()

	// Rename output file
	os.Rename(ofile.Name(), oname)

	// Update task status
	reply.Task.Status = dto.Done
	replyEx := dto.Reply{}
	utils.CallNotifyTaskDone("Master.NotifyTaskDone", reply, replyEx)
}

func DoMerge(reply dto.Reply, mergef func([]string, string)) {
	mergef(reply.Task.InputFiles, reply.Task.OutputFiles[0])
	reply.Task.Status = dto.Done
	replyEx := dto.Reply{}
	utils.CallNotifyTaskDone("Master.NotifyTaskDone", reply, replyEx)
}
