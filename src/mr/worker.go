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
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	var args Args
	var reply Reply

	for {
		Ready(&args, &reply)

		nReducers := reply.NReducers
		taskType := reply.TaskType
		filename := reply.Filename
		taskId := reply.TaskId

		switch taskType {
		case MAP:
			fmt.Printf("[WORKER] Assignment: Task ID: %v Task type: %s\n", taskId, MAP)
			content, err := os.ReadFile(filename)

			if err != nil {
				log.Printf("Cannot open %v: %v", filename, err)
				break
			}

			keyValue := mapFunction(filename, string(content))
			files := make([]*os.File, nReducers)

			for i := 0; i < int(nReducers); i++ {
				filename := fmt.Sprintf("mr-%v-%d", taskId, i)
				file, err := os.Create(filename)
				if err != nil {
					log.Printf("Cannot create file mr-%v-%d: %v", taskId, i, err)
					break
				}
				files[i] = file
			}

			for _, entry := range keyValue {
				reducerIndex := ihash(entry.Key) % int(nReducers)
				fmt.Fprintf(files[reducerIndex], "%v %v\n", entry.Key, entry.Value)
			}

			for _, f := range files {
				if f != nil {
					f.Close()
				}
			}

			responseArgs := Args{
				TaskType: MAP,
				TaskId:   taskId,
			}

			TaskDone(&responseArgs, nil)

		case REDUCE:
			fmt.Printf("[WORKER] Assignment: Task ID: %v Task type: %s\n", taskId, REDUCE)

			files, err := getReduceFiles(taskId)
			if err != nil {
				log.Printf("Cannot open reduce files: %v", err)
				break
			}

			var intermediate []KeyValue
			for _, file := range files {
				content, err := os.Open(file)
				if err != nil {
					continue
				}

				scanner := bufio.NewScanner(content)
				for scanner.Scan() {
					parts := strings.Fields(scanner.Text())
					if len(parts) < 2 {
						continue
					}
					word := parts[0]
					count := parts[1]
					intermediate = append(intermediate, KeyValue{word, count})
				}
				if err := scanner.Err(); err != nil {
					fmt.Println("Scanner error:", err)
				}
				content.Close() // Close file after reading
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", taskId)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Printf("Cannot create output file %v: %v", oname, err)
				break
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reduceFunction(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()

			sendDoneArgs := Args{
				TaskType: REDUCE,
				TaskId:   taskId,
			}

			TaskDone(&sendDoneArgs, nil)
		case FINISH:
			fmt.Println("[WORKER] Received FINISH signal, exiting.")
			return
		}

		reply.TaskId = 0
		reply.TaskType = ""
		time.Sleep(time.Second * 5)
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

func TaskDone(args *Args, reply *Reply) error {
	ok := call("Coordinator.TaskDone", &args, &reply)

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

func getReduceFiles(reduceNumber uint8) ([]string, error) {
	glob := fmt.Sprintf("mr-*%d", reduceNumber)
	files, err := filepath.Glob(glob)
	if err != nil {
		return nil, errors.New("cannot read the files")
	}

	return files, nil
}
