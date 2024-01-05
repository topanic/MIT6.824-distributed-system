package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"6.5840/tools"
	// "sync"
)


type Coordinator struct {
	// Your definitions here.

	Files []string

	MapTaskNum int	// the number of map task
	MapTaskRemainNum int // when a woker complete a map phase, this will minus 1.
	MapTaskCompleteNum int 

	ReduceTaskNum int // the number of reduce task
	ReduceTaskRemainNum int // when a woker complete a reduce phase, this will minus 1.
	ReduceTaskCompleteNum int 

	// if the task is complete
	MapComplete bool
	ReduceComplete bool

	// assign works and record if a task is complete
	MapTaskCompleteMap map[int]bool
	ReduceTaskCompleteMap map[int]bool
	MapTaskQueue tools.Queue
	ReduceTaskQueue tools.Queue

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Task(args *CallForTaskArgs, reply *CallForTaskReply) error {
	if !c.MapComplete && c.MapTaskRemainNum != 0 {
		// has map task
		taskNum := c.MapTaskQueue.Pop()
		c.MapTaskRemainNum--
		reply.TaskType = 'm'
		reply.Filename = c.Files[taskNum]
		reply.HasTask = true
		reply.TaskNum = taskNum
		reply.NReduce = c.ReduceTaskNum
		go c.traceTask('m', reply.TaskNum)
	} else if c.MapComplete && !c.ReduceComplete && c.ReduceTaskRemainNum != 0 {
		// has reduce task
		c.ReduceTaskRemainNum--
		reply.TaskType = 'r'
		reply.HasTask = true
		reply.TaskNum = c.ReduceTaskQueue.Pop()
		go c.traceTask('r', reply.TaskNum)
	} else {
		// not a map task or a reduce avaliable
		reply.HasTask = false
	}

	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, relpy *TaskCompleteReply) error {
	if args.TaskType == 'm' {
		c.MapTaskCompleteNum++
		c.MapTaskCompleteMap[args.TaskNum] = true
		return nil
	} else if args.TaskType == 'r' {
		c.ReduceTaskCompleteNum++
		c.ReduceTaskCompleteMap[args.TaskNum] = true
		return nil
	} else {
		return errors.New("Coordinator: get the wrong TaskComplete type. ")
	}
}

// be used for goroutine to trace the task, if worker crash, it can make sure task being add to the task queue
func (c *Coordinator) traceTask(taskType byte, taskNum int) {
	time.Sleep(10 * time.Second)
	if taskType == 'm' {
		value, ok := c.MapTaskCompleteMap[taskNum]
		if !ok {
			return
		}
		if !value {
			c.MapTaskQueue.Push(taskNum)
			c.MapTaskRemainNum++
		} 
	} else if taskType == 'r' {
		value, ok := c.ReduceTaskCompleteMap[taskNum]
		if !ok {
			return
		}
		if !value {
			c.ReduceTaskQueue.Push(taskNum)
			c.ReduceTaskRemainNum++
		} 
	} else {
		return
	}
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// justify if map phase is complete.
	if c.MapTaskCompleteNum == c.MapTaskNum {
		c.MapComplete = true
	}

	if c.ReduceTaskCompleteNum == c.ReduceTaskNum {
		c.ReduceComplete = true
	}

	if c.ReduceComplete {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.Files = files  // get the filename

	c.MapTaskRemainNum = len(files)
	c.MapTaskNum = len(files)
	c.MapTaskCompleteNum = 0
	c.MapComplete = false

	c.ReduceTaskRemainNum = nReduce
	c.ReduceTaskNum = nReduce
	c.ReduceTaskCompleteNum = 0
	c.ReduceComplete = false

	c.MapTaskCompleteMap = make(map[int]bool)
	c.ReduceTaskCompleteMap = make(map[int]bool)
	c.MapTaskQueue = tools.Queue{}
	c.ReduceTaskQueue = tools.Queue{}
	for i := 0; i < len(files); i++ {
		c.MapTaskCompleteMap[i] = false
		c.MapTaskQueue.Push(i)
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskCompleteMap[i] = false
		c.ReduceTaskQueue.Push(i)
	}

	c.server()
	return &c
}
