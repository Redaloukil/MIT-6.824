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

type Params struct {
	WorkerId  uint8
	taskId    uint8
	taskType  string
	nReducers uint8
}

type Args struct {
	params Params
}

type Reply struct {
	params Params
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
