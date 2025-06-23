package mr

import "testing"

func TestIsAllMapTasksDone(t *testing.T) {
	c := Coordinator{
		mapTasks: make([]uint8, 4),
	}

	c.mapTasks = []uint8{DONE, DONE, DONE, PENDING}
	isAllMapTasksDone := c.isAllMapTasksDone()

	if isAllMapTasksDone == true {
		t.Errorf("isAllMapTasksDone should return false")
	}

	c.mapTasks = []uint8{DONE, DONE, DONE, DONE}
	isAllMapTasksDone = c.isAllMapTasksDone()

	if isAllMapTasksDone == false {
		t.Errorf("isAllMapTasksDone should return false")
	}
}

func TestDone(t *testing.T) {
	c := Coordinator{
		mapTasks:    make([]uint8, 4),
		reduceTasks: make([]uint8, 2),
	}

	c.mapTasks = []uint8{DONE, DONE, DONE, DONE}
	c.reduceTasks = []uint8{DONE, DONE}

	done := c.Done()

	if done == false {
		t.Errorf("Done should return true")
	}

	c.mapTasks = []uint8{DONE, DONE, RUNNING, DONE}
	c.reduceTasks = []uint8{PENDING, PENDING}

	done = c.Done()

	if done == true {
		t.Errorf("Done should return false")
	}
}

func TestReadyRpc(t *testing.T) {
	c := Coordinator{
		mapTasks:    make([]uint8, 4),
		reduceTasks: make([]uint8, 2),
	}

	c.mapTasks = []uint8{PENDING, PENDING, PENDING, PENDING}
	c.reduceTasks = []uint8{PENDING, PENDING}

	args := Args{}
	reply := Reply{}

	c.Ready(&args, &reply)

	if reply.params.taskType == REDUCE {
		t.Errorf("task type should return true")
	}

	if c.mapTasks[0] == PENDING {
		t.Errorf("first map task should not be PENDING")
	}

	c.mapTasks = []uint8{DONE, DONE, DONE, DONE}
	c.reduceTasks = []uint8{RUNNING, PENDING}

	args = Args{}
	reply = Reply{}

	c.Ready(&args, &reply)

	if c.reduceTasks[1] == PENDING {
		t.Errorf("first reduce task should not be PENDING")
	}

}
