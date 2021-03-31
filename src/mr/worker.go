package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const (
	register = "Coordinator.Register"
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	workerId := Register()
	fmt.Println("Currently in the Worker ", workerId)

	AskForTask(mapf)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func Register() int {
	reply := RegisterReply{}
	_ = call(register, RegisterArgs{}, reply)
	return reply.WorkerId
}

func Ping() {

}

func AskForTask(mapf func(string, string) []KeyValue) {
	args := Idle{}
	args.Idle = true
	reply := ReplyMap{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ArrangeTask", &args, &reply)
	fmt.Println(reply)

	if reply.AllArranged {
		fmt.Println("All Files had been arranged to Map workers!")
		return
	}

	nReduce := reply.NReduce

	id := reply.Id

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	_ = file.Close()
	kva := mapf(reply.FileName, string(content))

	var files []*os.File
	var encs []*json.Encoder
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		interName := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(reduceId)
		interFile, _ := os.Create(interName)
		enc := json.NewEncoder(interFile)
		encs = append(encs, enc)
		files = append(files, interFile)
	}

	for _, kv := range kva {
		key := kv.Key
		//value := kv.Value
		hash := ihash(key) % nReduce
		err := encs[hash].Encode(&kv) //Todo: is & needed? try it
		if err != nil {
			log.Fatal("Error", err)
		}
	}

	for _, file := range files {
		file.Close()
	}
	//fmt.Printf("reply.ReplyMap %v\n", kva)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
