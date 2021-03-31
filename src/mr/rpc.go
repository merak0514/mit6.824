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
	Msg string // useless but for fun
}

type ReplyMap struct {
	AllArranged bool
	FileName    string
	Id          int
	NReduce     int
}

type Idle struct {
	Idle bool
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
