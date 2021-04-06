package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"
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
// use ihash(key) % nReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const (
	register = "Coordinator.Register"
	ping     = "Coordinator.Ping"
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	endFlag := false
	endFlagLock := sync.Mutex{}
	workerId := Register()
	fmt.Println("Currently in the Worker ", workerId)

	go func() { // 每一秒打一次乒乓
		for {
			endFlagLock.Lock()
			if endFlag == true {
				endFlagLock.Unlock()
				break
			}
			endFlagLock.Unlock()
			Ping(workerId)
			time.Sleep(1 * time.Second)
		}
	}()
	done := false
	lastTask := 0 // 0 no task; 1 map; 2 reduce
	for !done {
		time.Sleep(1 * time.Second)
		done = AskForTask(mapf, workerId, &lastTask)
		fmt.Println("lastTask: ", lastTask)
	}
	//time.Sleep(3*time.Second)
	endFlagLock.Lock()
	endFlag = true
	endFlagLock.Unlock()
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
	fmt.Println("Trying to Register")
	reply := RegisterReply{}
	_ = call(register, RegisterArgs{}, &reply) // 没有加 & 就会出问题，可以试试。
	fmt.Println("Register successfully， with Worker id", reply.WorkerId)
	return reply.WorkerId
}

func Ping(workerId int) {
	reply := PingReply{}
	_ = call(ping, PingArgs{WorkerId: workerId}, &reply)
	fmt.Println(reply.Msg)
}

func DoMap(mapf func(string, string) []KeyValue, reply ReplyTaskInfo) {
	nReduce := reply.NReduce

	mapTaskId := reply.MapTaskId

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
		interName := "map_file/mr-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(reduceId)
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
	fmt.Println("Finished one Map")

}

func DoReduce(mapf func(string, string) []KeyValue, reply ReplyTaskInfo) {}

func AskForTask(mapf func(string, string) []KeyValue, workerId int, lastTask *int) bool {
	fmt.Println("Asking for a task")
	args := ArgsTask{WorkerId: workerId, LastTask: *lastTask}
	*lastTask = 0
	reply := ReplyTaskInfo{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ArrangeTask", &args, &reply)
	fmt.Println(reply)

	switch reply.TaskType {
	case 0:
		return false
	case 1: //map
		DoMap(mapf, reply)
		*lastTask = 1
	case 2:
		DoReduce(mapf, reply)
		*lastTask = 2
	case -1:
		return true
	}
	return false

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
