package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const N_REDUCERS = 3

type Coordinator struct {
	mapTasks       []uint8
	reduceTasks    []uint8
	nReducers      uint8
	workersCounter uint8
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Ready(args *Args, reply *Reply) error {
	// create workerId
	var workerId uint8

	if args.WorkerId == 0 {
		workerId = c.workersCounter
		c.workersCounter = c.workersCounter + 1
	} else {
		workerId = args.WorkerId
	}

	fmt.Printf("Worker with ID %v is Ready\n", workerId)

	// check if there is a ready map task
	if !c.isAllMapTasksDone() {
		for key, task := range c.mapTasks {
			if task == PENDING {
				reply.WorkerId = workerId
				reply.TaskType = MAP
				reply.TaskId = uint8(key)
				reply.NReducers = N_REDUCERS
				c.mapTasks[key] = RUNNING
				fmt.Printf("Assignment\n - Worker ID: %v\n - TaskID:%v\n - Task type: %s\n", workerId, key, MAP)
				return nil
			}
		}
	} else if c.isAllMapTasksDone() && !c.isAllReduceTasksDone() {
		for key, task := range c.reduceTasks {
			if task == PENDING {
				reply.WorkerId = workerId
				reply.TaskType = REDUCE
				reply.TaskId = uint8(key)
				reply.NReducers = N_REDUCERS

				c.reduceTasks[key] = RUNNING

				fmt.Printf("Sending Worker with ID %v %v task with TaskID %v\n", workerId, REDUCE, key)
				return nil
			}

		}
	} else if c.isAllMapTasksDone() && c.isAllReduceTasksDone() {
		reply.TaskType = FINISH
		return nil
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.isAllReduceTasksDone() && c.isAllMapTasksDone() {
		ret = true
	}
	// Your code here.

	return ret
}

// check if all the map tasks are done
func (c *Coordinator) isAllMapTasksDone() bool {
	for _, status := range c.mapTasks {
		if status != DONE {
			return false
		}
	}

	return true
}

func (c *Coordinator) isAllReduceTasksDone() bool {
	for _, status := range c.reduceTasks {
		if status != DONE {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapTasks = make([]uint8, len(files))
	c.reduceTasks = make([]uint8, nReduce)
	c.nReducers = uint8(nReduce)

	// Your code here.
	for key := range files {
		c.mapTasks[key] = PENDING
	}

	for key := range c.reduceTasks {
		c.reduceTasks[key] = PENDING
	}

	c.workersCounter = 1

	c.server()
	return &c
}
