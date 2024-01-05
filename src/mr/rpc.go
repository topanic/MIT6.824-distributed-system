package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


type CallForTaskArgs struct {

}

type CallForTaskReply struct {
	TaskType byte // 'm' means map, 'r' means reduce
	TaskNum int // be used to name the intermediate files

	Filename string  // the file that work should 'mapf'
	NReduce int	 // will divide kv into 'NReduce' intermediate files

	HasTask bool // make sure if a task is avaliable
}

type TaskCompleteArgs struct {
	TaskType byte // 'm' means map, 'r' means reduce
	TaskNum int  // the task number worker complete
}

type TaskCompleteReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
