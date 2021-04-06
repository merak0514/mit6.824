package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerId int
}

type PingArgs struct {
	WorkerId int
}

type PingReply struct {
	Msg    string // useless but for fun
	status int    // 在server看来worker的状态：0 idle, 1 map, 2 reduce
}

type ReplyTaskInfo struct {
	AllArranged bool
	FileName    string
	WorkerId    int
	NReduce     int
	TaskType    int // 0 no task; 1 map; 2 reduce; -1 all done
	MapTaskId   int
}

type ArgsTask struct {
	WorkerId int
	FileName string
	LastTask int // 0 no task; 1 map; 2 reduce
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
