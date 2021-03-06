package mr

import (
	"fmt"
	"github.com/fatih/color"
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
	workerId      int
	lastSeen      int
	alive         int // -1 die; 0 idle
	mission       int // 0 no mission, 1 map, 2 reduce
	operatingFile int
	mu            sync.Mutex
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

func (c *Coordinator) dealingMapDie(ws *workerStatus) {
	mappingFile := ws.operatingFile
	c.mu.Lock()
	c.filesMapQueue = append(c.filesMapQueue, mappingFile)
	c.mu.Unlock()
}
func (c *Coordinator) dealingReduceDie(ws *workerStatus) {
	reducingFile := ws.operatingFile
	c.mu.Lock()
	c.reduceQueue = append(c.reduceQueue, reducingFile)
	c.mu.Unlock()
}
func (c *Coordinator) dealingWorkerDie(ws *workerStatus) {
	fmt.Println("Dealing the Death")
	originMission := ws.mission
	fmt.Println("originMission", originMission)
	switch originMission {
	case 1:
		c.dealingMapDie(ws)
	case 2:
		c.dealingReduceDie(ws)
	}
}

func (ws *workerStatus) longTimeNoSee(c *Coordinator) {
	for {
		time.Sleep(1 * time.Second)
		ws.mu.Lock()
		ws.lastSeen++
		if ws.lastSeen == 10 { //超过10s标记为死亡
			ws.alive = -1
			c.dealingWorkerDie(ws) // 处理后事
		}
		ws.mu.Unlock()
	}
}

type Coordinator struct {
	// Your definitions here.
	files         []string // 文件，初始化后不更改
	filesMapQueue []int    // 文件队列，其中用id代表每个文件

	reduceQueue []int

	nReduce           int
	finishedTaskCount int // count for map or reduce, depending on phase
	mapDone           bool
	reduceDone        bool
	workerIdCount     int
	workerTable       map[int]*workerStatus
	mapTable          map[int]*mapStatus
	reduceTable       map[int]*reduceStatus

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
	c.workerIdCount++
	reply.WorkerId = c.workerIdCount
	workerStatus := workerStatus{workerId: c.workerIdCount, lastSeen: 0, alive: 0, mission: 0}
	c.workerTable[c.workerIdCount] = &workerStatus
	c.mu.Unlock()
	go workerStatus.longTimeNoSee(c)
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
	if worker.alive == -1 { //重新收到信号的时候标记已经"死亡"的worker为空闲
		worker.alive = 0
	}
	//fmt.Println(worker.lastSeen)
	worker.mu.Unlock()
	return nil
}

func (c *Coordinator) arrangeMap(args *ArgsTask, reply *ReplyTaskInfo) error {
	reply.TaskType = 1 // Map

	mappingFile := c.filesMapQueue[0]
	reply.FileName = c.files[mappingFile]
	reply.MapTaskId = mappingFile
	c.filesMapQueue = c.filesMapQueue[1:]

	ws := c.workerTable[args.WorkerId]
	ws.mu.Lock()
	ws.operatingFile = mappingFile
	ws.mu.Unlock()
	return nil
}

func (c *Coordinator) arrangeReduce(args *ArgsTask, reply *ReplyTaskInfo) error {
	reply.TaskType = 2 // Reduce

	reducingFile := c.reduceQueue[0]
	reply.ReduceTaskId = reducingFile
	c.reduceQueue = c.reduceQueue[1:]

	ws := c.workerTable[args.WorkerId]
	ws.mu.Lock()
	ws.operatingFile = reducingFile
	ws.mu.Unlock()
	return nil
}

func (c *Coordinator) ArrangeTask(args *ArgsTask, reply *ReplyTaskInfo) error {
	if args.LastTask == 1 { // Finished one map task
		c.mu.Lock()
		c.finishedTaskCount++
		if c.finishedTaskCount == len(c.files) {
			c.mapDone = true
			color.Set(color.FgGreen)
			fmt.Println("Map finished")
			color.Unset()
			c.finishedTaskCount = 0
		}
		c.mu.Unlock()
	} else if args.LastTask == 2 {
		c.mu.Lock()
		c.finishedTaskCount++
		if c.finishedTaskCount == c.nReduce {
			c.reduceDone = true
			color.Set(color.FgGreen)
			fmt.Println("Reduce finished")
			color.Unset()
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = args.WorkerId
	reply.NReduce = c.nReduce
	reply.NMap = len(c.files)

	lenMapQueue := len(c.filesMapQueue)
	lenReduceQueue := len(c.reduceQueue)
	mapDoneLocal := c.mapDone
	reduceDoneLocal := c.reduceDone
	if lenMapQueue > 0 { // Map还没分配完成
		fmt.Println("MapQueue", c.filesMapQueue)
		ws := c.workerTable[args.WorkerId]
		ws.mu.Lock()
		ws.mission = 1
		ws.mu.Unlock()
		return c.arrangeMap(args, reply)
	}
	if mapDoneLocal { // Map过程完成
		fmt.Println("ReduceQueue", c.reduceQueue)
		if lenReduceQueue > 0 { //  且nReduce没有到上限到时候
			c.workerTable[args.WorkerId].mission = 2
			return c.arrangeReduce(args, reply)
		} else { //  Map 完成且nReduce到上限
			if reduceDoneLocal { // Map 和Reduce都完成，让worker去死
				reply.TaskType = -1
			} else { //  Map 完成且nReduce到上限，但reduce没完成；这时候再接入的worker要让他们等待（因为有可能出现某个Reduce挂了）
				reply.TaskType = 0
			}
		}
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
	c.mu.Lock()
	if c.reduceDone && c.mapDone {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// make a Range for [min, max)
func makeRange(min, max int) []int {
	a := make([]int, max-min)
	for i := range a {
		a[i] = min + i
	}
	return a
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
	//filesQueue := makeRange(0, len(files))

	c := Coordinator{
		files:             files,
		filesMapQueue:     makeRange(0, len(files)),
		mapDone:           false,
		nReduce:           nReduce,
		reduceDone:        false,
		workerIdCount:     0,
		reduceQueue:       makeRange(0, nReduce),
		finishedTaskCount: 0,
	}
	c.init()

	for i := 0; i <= nReduce; i++ {

	}

	c.server()
	return &c
}
