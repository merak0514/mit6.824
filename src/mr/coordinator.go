package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type WorkerStatus struct {
	workerId int
	lastSeen int
	status   int // -1 die; 0 idle
	mu       sync.Mutex
}

func (ws *WorkerStatus) longTimeNoSee() {
	for {
		time.Sleep(1 * time.Second)
		ws.mu.Lock()
		ws.lastSeen++
		if ws.lastSeen == 10 { //超过10s标记为死亡
			ws.status = -1
		}
		ws.mu.Unlock()

	}

}

type Coordinator struct {
	// Your definitions here.
	Files    []string
	position int
	nReduce  int
	mapDone  sync.WaitGroup
	idCount  int
	workers  map[int]*WorkerStatus
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
	workerStatus := WorkerStatus{workerId: c.idCount, lastSeen: 0}
	go workerStatus.longTimeNoSee()
	c.workers[c.idCount] = &workerStatus

	c.mu.Lock()
	defer c.mu.Unlock()
	c.idCount++
	return nil
}

// Sanity check
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	reply.Msg = "Pong" // useless but for fun
	worker := c.workers[args.WorkerId]

	worker.mu.Lock()
	worker.lastSeen = 0
	if worker.status == -1 { //重新收到信号的时候标记已经"死亡"的worker为空闲
		worker.status = 0
	}
	worker.mu.Unlock()
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
