package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
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

type WorkerArgs struct {
	// 0 ask for a new task
	// 1 report map task finished
	// 2 report reduce task finished
	Task int8
	X    int
}

func (w WorkerArgs) String() string {
	return fmt.Sprintf("WorkerArgs{Task: %v, X: %v}", w.Task, w.X)
}

type CoordinatorReply struct {
	// -3 exit immediately
	// -2 request immediately
	// -1 request later
	// 0 Map task
	// 1 Reduce task
	Task int8

	// Task == 0
	X       int
	File    string
	NReduce int

	// Task == 1
}

func (c CoordinatorReply) String() string {
	return fmt.Sprintf("CoordinatorReply{Task: %v, X: %v, File: %v, NReduce: %v}", c.Task, c.X, c.File, c.NReduce)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
