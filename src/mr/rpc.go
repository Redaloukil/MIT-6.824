package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	WorkerId uint8
	TaskId   uint8
	TaskType string
}

type Reply struct {
	WorkerId  uint8
	TaskId    uint8
	TaskType  string
	NReducers uint8
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

const (
	PENDING = iota
	RUNNING
	DONE
)

const MAP = "MAP"
const REDUCE = "REDUCE"
const FINISH = "FINISH"
