package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

//var m map[int]*workerStatus

type workerStatus struct {
	workerId int
	lastSeen int
	status   int // -1 die; 0 idle
	mu       sync.Mutex
}
type mapStatus struct {
	fileName string
	workerId int
	status   int // 0 not arranged; 1 dealing; 2 done
	mu       sync.Mutex
}
type reduceStatus struct {
	workerId int

	mu sync.Mutex
}

func (ws *workerStatus) longTimeNoSee() {
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
	Files               []string
	filePosition        int
	nReduce             int
	arrangedReduceCount int
	mapWaitGroup        sync.WaitGroup
	mapDone             bool
	reduceDone          bool
	workerIdCount       int
	workerTable         map[int]*workerStatus
	mapTable            map[int]*mapStatus
	reduceTable         map[int]*reduceStatus

	mu sync.Mutex
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

func (c *Coordinator) init() {
	c.workerTable = make(map[int]*workerStatus)
	c.mapTable = make(map[int]*mapStatus)
	c.reduceTable = make(map[int]*reduceStatus)
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	workerIdCountLocal := c.workerIdCount
	c.workerIdCount++
	reply.WorkerId = workerIdCountLocal
	workerStatus := workerStatus{workerId: workerIdCountLocal, lastSeen: 0}
	c.workerTable[workerIdCountLocal] = &workerStatus
	c.mu.Unlock()
	go workerStatus.longTimeNoSee()

	return nil
}

// Sanity check
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	reply.Msg = "Pong" // useless but for fun
	c.mu.Lock()
	worker := c.workerTable[args.WorkerId]
	c.mu.Unlock()

	worker.mu.Lock()
	worker.lastSeen = 0
	if worker.status == -1 { //重新收到信号的时候标记已经"死亡"的worker为空闲
		worker.status = 0
	}
	//fmt.Println(worker.lastSeen)
	worker.mu.Unlock()
	return nil
}

func (c *Coordinator) arrangeMap(args *ArgsTask, reply *ReplyTaskInfo) error {
	reply.TaskType = 1 // Map
	reply.WorkerId = args.WorkerId
	reply.NReduce = c.nReduce
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.FileName = c.Files[c.filePosition]
	c.filePosition++
	c.mapWaitGroup.Add(1)
	return nil
}

func (c *Coordinator) arrangeReduce(args *ArgsTask, reply *ReplyTaskInfo) error {
	return nil
}

func (c *Coordinator) ArrangeTask(args *ArgsTask, reply *ReplyTaskInfo) error {
	if args.LastTask == 1 { // Finished one map task
		c.mu.Lock()
		c.mapWaitGroup.Done()
		c.mu.Unlock()
	}
	c.mu.Lock()
	filePositionLocal := c.filePosition
	arrangedReduceCountLocal := c.arrangedReduceCount
	mapDoneLocal := c.mapDone
	reduceDoneLocal := c.reduceDone
	c.mu.Unlock()
	if filePositionLocal < len(c.Files) { // Map还没分配完成
		return c.arrangeMap(args, reply)
	}
	if mapDoneLocal { // Map过程完成
		reply.TaskType = -1 //  tmp for testing
		return nil

		if arrangedReduceCountLocal < c.nReduce { //  且nReduce没有到上限到时候
			c.mu.Lock()
			c.arrangedReduceCount++
			c.mu.Unlock()
			return c.arrangeReduce(args, reply)
		} else { //  Map 完成且nReduce到上限
			if reduceDoneLocal { // Map 和Reduce都完成，让worker去死
				reply.TaskType = -1
			} else { //  Map 完成且nReduce到上限，但reduce没完成；这时候再接入的worker要让他们等待（因为有可能出现某个Reduce挂了）
				reply.TaskType = 0
			}
		}
	} else {
		fmt.Println(*c)
	}

	reply.TaskType = 0

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
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Printf("%v\n", files)
	fmt.Println(nReduce)

	os.Mkdir("map_file/", os.ModePerm)

	c := Coordinator{Files: files, filePosition: 0, mapDone: false, nReduce: nReduce, reduceDone: false}
	c.init()
	go func() { // 检测map是否完成的线程
		var filePositionLocal int
		for {
			//time.Sleep(1*time.Second)

			c.mapWaitGroup.Wait()
			c.mu.Lock()
			filePositionLocal = c.filePosition
			c.mu.Unlock()
			if filePositionLocal >= len(c.Files) {
				c.mu.Lock()
				c.mapDone = true
				c.mu.Unlock()
				break
			}

		}
	}()

	// Your code here.
	for i := 0; i <= nReduce; i++ {

	}

	c.server()
	return &c
}
