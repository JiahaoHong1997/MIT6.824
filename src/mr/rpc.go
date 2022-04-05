package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
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

// Add your RPC definitions here.
// Ask for WorkerNum and Contruct Connection
type HelloArgs struct {
	X string
}

type HelloReply struct {
	Y int
}

// Map tasks Arguments and Reply
type MapArgs struct {
	WorkerNum int
}

type MapReply struct {
	NReducer int
	Finished bool
	FileName string
}

// Finish Map Task
type FinishMapArgs struct {
	X         bool
	WorkerNum int
	FileName  []string
}

type FinishMapReply struct {
	Y int
}

// Reduce Task
type ReduceTaskArgs struct {
	WorkerNum int
}

type ReduceTaskReply struct {
	Finished    bool
	ReducerFile []string
}

// Finish Reduce Task
type FinishReduceArgs struct {
	X         bool
	WorkerNum int
	File      string
}

type FinishReduceReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
