package utils

import (
	"fmt"
	"log"
	"mapreduce_worker/internal/dto"
	"net/rpc"
)

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func Call(rpcname string, args dto.Args, reply *dto.Reply) bool {
	c, err := rpc.DialHTTP("tcp", "localhost"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, &args, &reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func CallNotifyTaskDone(rpcname string, args dto.Reply, reply dto.Reply) bool {
	c, err := rpc.DialHTTP("tcp", "localhost"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, &args, &reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
