package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {

	var args Args
	var reply Reply
	var workerId uint8

	for {
		Ready(&args, &reply)

		if workerId == 0 {
			fmt.Printf("Worker ID %v has been assigned to the process\n", reply.WorkerId)
		}

		workerId = reply.WorkerId
		nReducers := reply.NReducers
		taskType := reply.TaskType
		taskId := reply.TaskId

		switch taskType {
		case MAP:
			fmt.Printf("Assignment\n - Task ID: %v\n - Task type: %s\n", taskId, MAP)

			fileNames, err := getMapFiles()
			if err != nil {
				log.Fatalf("Cannot open files")
				break
			}

			content, err := os.ReadFile(fileNames[taskId])
			if err != nil {
				log.Fatalf("Cannot open %v", fileNames[taskId])
				break
			}

			keyValue := mapFunction(fileNames[taskId], string(content))
			files := make([]*os.File, nReducers)

			for i := 0; i < int(nReducers); i++ {
				filename := fmt.Sprintf("mr-%v-%d", taskId, i)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("Cannot create file mr-%v-%d", taskId, i)
					break
				}
				files[i] = file
			}

			for _, entry := range keyValue {
				reducerIndex := ihash(entry.Key) % int(nReducers)
				fmt.Fprintf(files[reducerIndex], "%v %v\n", entry.Key, entry.Value)
			}

			args.WorkerId = workerId
			args.TaskType = MAP
			args.TaskId = taskId

		case REDUCE:
			fmt.Printf("Assignment\n - Task ID: %v\n - Task type: %s\n", taskId, REDUCE)

			files, err := getReduceFiles(taskId)
			if err != nil {
				log.Fatalf("Cannot open files")
				break
			}

			for _, file := range files {
				f, err := os.Open(file)
				if err != nil {
					break
				}
				defer f.Close()

				scanner := bufio.NewScanner(f)

				parts := strings.Fields(scanner.Text()) // splits by spaces
				if len(parts) != 2 {
					continue
				}

				word := parts[0]
				count, err := strconv.Atoi(parts[1])

				if err != nil {
					fmt.Printf("Invalid count in file %s: %v\n", file, err)
					continue
				}

				wordCount[word] += count
			}

			scanner := bufio.NewScanner(content)

			args.WorkerId = workerId
			args.TaskType = REDUCE
			args.TaskId = taskId

		}
	}
}

// example function to show how to make an RPC call to the coordinator.
func Ready(args *Args, reply *Reply) error {
	ok := call("Coordinator.Ready", &args, &reply)

	if ok {
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func getMapFiles() ([]string, error) {
	files, err := filepath.Glob("pg-*.txt")
	if err != nil {
		return nil, errors.New("cannot read the files")
	}

	return files, nil
}

func getReduceFiles(reduceNumber uint8) ([]string, error) {
	glob := fmt.Sprintf("mr-*-%d.txt", reduceNumber)
	files, err := filepath.Glob(glob)
	if err != nil {
		return nil, errors.New("cannot read the files")
	}

	return files, nil
}
