package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Files    []string
	position int
	nReduce  int
	mapDone  sync.WaitGroup
	idCount  int
	workers  []int
	mu       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.WorkerId = c.idCount
	c.workers = append(c.workers, c.idCount)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idCount++
	return nil
}

func (c *Coordinator) PingPong() error {
	return nil
}

func (c *Coordinator) ArrangeTask(args *Idle, reply *ReplyMap) error {
	if c.position >= len(c.Files) {
		reply.AllArranged = true
		return nil
	}
	reply.FileName = c.Files[c.position]
	reply.Id = c.position
	reply.NReduce = c.nReduce
	c.position++

	c.mapDone.Add(1)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	fmt.Printf("%v\n", files)
	fmt.Println(nReduce)
	c := Coordinator{Files: files, position: 0, nReduce: nReduce}

	// Your code here.
	for i := 0; i <= nReduce; i++ {

	}

	c.server()
	return &c
}
