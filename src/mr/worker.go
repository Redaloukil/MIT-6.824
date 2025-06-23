package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var workerId uint8
	var nReducers uint8

	params := Ready()

	workerId = params.WorkerId
	nReducers = params.nReducers
	taskType := params.taskType

	switch taskType {
	case MAP:
		fileNames := getMapFiles()
		content, err := os.ReadFile(fileNames[params.taskId])
		if err != nil {
			log.Fatalf("Cannot open %v", fileNames[params.taskId])
			break
		}

		keyValue := mapf(fileNames[params.taskId], string(content))
		files := make([]*os.File, nReducers)

		for i := 0; i < int(nReducers); i++ {
			filename := fmt.Sprintf("mr-%v-%d", params.taskId, i)
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalf("Cannot create file mr-%v-%d", params.taskId, i)
				break
			}

			files[i] = file
		}

		for _, entry := range keyValue {
			reducerIndex := ihash(entry.Key) % int(nReducers)
			fmt.Fprintf(files[reducerIndex], "%v %v", entry.Key, entry.Value)
		}

	case REDUCE:
		break
	default:
		break
	}
}

// example function to show how to make an RPC call to the coordinator.
func Ready() Params {
	reply := Reply{}

	ok := call("Coordinator.Ready", nil, &reply)

	if ok {
		return reply.params
	}

	fmt.Printf("call failed!\n")
	return Params{}
}

// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		return
	} else {
		fmt.Printf("call failed!\n")
	}
}

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

func getMapFiles() []string {
	files, err := filepath.Glob("pg-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	return files
}
