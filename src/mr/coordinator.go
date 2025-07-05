package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	files       []string
	mapTasks    []uint8
	reduceTasks []uint8
	nReducers   uint8
	mu          sync.Mutex
}

func (c *Coordinator) Ready(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.allMapTasksAreDone() {
		for key, task := range c.mapTasks {
			if task == PENDING {
				reply.TaskType = MAP
				reply.TaskId = uint8(key)
				reply.NReducers = c.nReducers
				reply.Filename = c.files[key]

				c.mapTasks[key] = RUNNING

				fmt.Printf("Assigning : TaskID: %v Task type: %s\n", reply.TaskId, MAP)

				return nil
			}
		}

		return nil
	}

	if c.allMapTasksAreDone() && !c.allReduceTasksAreDone() {
		for key, task := range c.reduceTasks {
			if task == PENDING {
				reply.TaskType = REDUCE
				reply.TaskId = uint8(key)
				reply.NReducers = c.nReducers
				c.reduceTasks[key] = RUNNING

				fmt.Printf("Assigning : TaskID: %v Task type: %s\n", reply.TaskId, REDUCE)
				return nil
			}
		}

		return nil
	}

	reply.TaskType = FINISH
	reply.TaskId = uint8(0)

	return nil
}

func (c *Coordinator) TaskDone(args *Args, reply *Reply) error {
	fmt.Printf("Assignment Done: TaskID: %v Task type: %s\n", args.TaskId, args.TaskType)
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MAP:
		c.mapTasks[args.TaskId] = DONE

	case REDUCE:
		c.reduceTasks[args.TaskId] = DONE
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

func (c *Coordinator) Done() bool {
	ret := false

	if c.allReduceTasksAreDone() && c.allMapTasksAreDone() {
		ret = true
	}

	return ret
}

// check if all the map tasks are done
func (c *Coordinator) allMapTasksAreDone() bool {
	for _, status := range c.mapTasks {
		if status != DONE {
			return false
		}
	}

	return true
}

func (c *Coordinator) allReduceTasksAreDone() bool {
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
	c.files = files
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

	c.server()

	return &c
}
