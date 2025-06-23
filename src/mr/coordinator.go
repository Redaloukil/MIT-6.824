package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	mapTasks       []uint8
	reduceTasks    []uint8
	nReducers      uint8
	workersCounter uint8
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *Args, reply *Reply) error {
	return nil
}

func (c *Coordinator) Ready(args *Args, reply *Reply) error {
	// create workerId
	workerId := c.workersCounter
	c.workersCounter = c.workersCounter + 1
	// check if there is a ready map task
	if !c.isAllMapTasksDone() {
		for key, task := range c.mapTasks {
			if task == PENDING {
				reply.params = Params{
					WorkerId: workerId,
					taskType: MAP,
					taskId:   uint8(key),
					nReducers: ,
				}

				c.mapTasks[key] = RUNNING
				return nil
			}
		}
	} else if !c.isAllReduceTasksDone() {
		for key, task := range c.reduceTasks {
			if task == PENDING {
				reply.params = Params{
					WorkerId: workerId,
					taskType: REDUCE,
					taskId:   uint8(key),
				}

				c.reduceTasks[key] = RUNNING
				return nil
			}

		}
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
	for range files {
		c.mapTasks = append(c.mapTasks, PENDING)
	}

	for range c.nReducers {
		c.reduceTasks = append(c.reduceTasks, PENDING)
	}

	c.workersCounter = 0

	c.server()
	return &c
}
