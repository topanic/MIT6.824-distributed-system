package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		if reply := CallForTask(); reply.HasTask {
			if reply.TaskType == 'm' {
				intermediate := doMap(reply.Filename, mapf)
				saveIntermediate(intermediate, reply.TaskNum, reply.NReduce)
				err := CallTaskComplete('m', reply.TaskNum)
				if err != nil {
					log.Fatalln("Worker: submit Map phase complete wrong")
				}
				continue
			} else if reply.TaskType == 'r' {
				doReduce(reply.TaskNum, reducef)
				err := CallTaskComplete('r', reply.TaskNum)
				if err != nil {
					log.Fatalln("Worker: submit Reduce phase complete wrong")
				}
				continue
			} else {
				log.Fatalln("Worker: TaskType error")
			}
			
		}
		time.Sleep(2 * time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}



func CallForTask() *CallForTaskReply {
	args := CallForTaskArgs{}
	reply := CallForTaskReply{}

	// try 3 times
	for i := 0; i < 3; i++ {
		if ok := call("Coordinator.Task", &args, &reply); ok {
			return &reply
		} 
		time.Sleep(time.Second * 1)
	}

	// work can not contact with coordinator, exit from work process
	os.Exit(0)
	return nil
}

func CallTaskComplete(taskType byte, taskNum int) error {
	args := TaskCompleteArgs{
		TaskType: taskType,
		TaskNum: taskNum,
	}
	reply := TaskCompleteReply{}
	if ok := call("Coordinator.TaskComplete", &args, &reply); ok {
		return nil
	} else {
		return errors.New("Worker: Call task complete error")
	}
} 

// map function
func doMap(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := []KeyValue{}
	content, _ := readFileContent(filename)
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

// reduce function
func doReduce(taskNum int, reducef func(string, []string) string) {
	// get the all keyValue
	files, err := filepath.Glob(fmt.Sprintf("mr*-%d", taskNum))
	if err != nil {
		log.Fatalln("Reduce: glob intermediate error. ")
	}
	kva := make([]KeyValue, 0)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalln("Reduce: Failed to open file:", err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
	// start to reduce
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", taskNum)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}


// tool function
//
// be used to replace "ioutil.ReadAll()"
func readFileContent(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("ReadFile: Failed to open file:", err)
		return nil, err
	}
	defer file.Close()

	// get the file size
	stat, err := file.Stat()
	if err != nil {
		log.Fatalln("ReadFile: Failed to get file size:", err)
		return nil, err
	}

	// alloc enough memory
	bytes := make([]byte, stat.Size())

	// read the file context
	_, err = io.ReadFull(file, bytes)
	if err != nil {
		log.Fatalln("ReadFile: Failed to read file:", err)
		return nil, err
	}
	
	return bytes, nil
}

// save the intermediate produced by the map phase into intermediate files 
func saveIntermediate(intermediate []KeyValue, mapTaskNum, nReduce int) {

	var m = make(map[int]*json.Encoder)
	for i := 0; i < nReduce; i++ {
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", mapTaskNum, i))
		if err != nil {
			log.Fatalln("CreateFile: Failed to create file:", err)
		}
		m[i] = json.NewEncoder(f)
	}
	for _, kv := range intermediate {
		err := m[ihash(kv.Key) % nReduce].Encode(&kv)
		if err != nil {
			log.Fatalln("Encode: Failed to encode KeyValue:", err)
		}
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
