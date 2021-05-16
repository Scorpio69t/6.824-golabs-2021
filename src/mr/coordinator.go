package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MRTask   // the length is the number of files
	reduceTasks []MRTask   // the length is nReduce
	finished    int        // the number of tasks finished
	lock        sync.Mutex // to protect the shared memory
}

// Your code here -- RPC handlers for the worker to call.

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
	return c.finished == len(c.mapTasks)+len(c.reduceTasks)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.finished = 0

	c.mapTasks = make([]MRTask, len(files))
	for i, f := range files {
		c.mapTasks[i] = MRTask{
			Type:   MapType,
			Number: i + 1,
			File:   f,
			Status: StatusNotStart,
		}
	}

	c.reduceTasks = make([]MRTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = MRTask{
			Type:   ReduceType,
			Number: i + 1,
			File:   "",
			Status: StatusNotStart,
		}
	}

	c.lock = sync.Mutex{}

	c.server()
	return &c
}

func (c *Coordinator) deferCheck(task []MRTask, number int, timeout time.Duration) {
	time.Sleep(timeout)
	c.lock.Lock()
	if task[number-1].Status != StatusFinished {
		task[number-1].Status = StatusNotStart
	} else {
		c.finished++
	}
	c.lock.Unlock()
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	if c.Done() {
		reply = &AskForTaskReply{Code: CodeAllTasksFinished}
		return nil
	}

	// find avaible tasks
	reply.NMap = len(c.mapTasks)
	reply.NReduce = len(c.reduceTasks)
	c.lock.Lock()
	defer c.lock.Unlock()

	allMapTasksFinished := true
	for i, v := range c.mapTasks {
		if v.Status == StatusNotStart {
			c.mapTasks[i].Status = StatusRunning
			reply.Code = CodeOk
			reply.Task = c.mapTasks[i]
			go c.deferCheck(c.mapTasks, v.Number, 10*time.Second)
			return nil
		} else if v.Status == StatusRunning {
			allMapTasksFinished = false
		}
	}

	if allMapTasksFinished {
		for i, v := range c.reduceTasks {
			if v.Status == StatusNotStart {
				c.reduceTasks[i].Status = StatusRunning
				reply.Code = CodeOk
				reply.Task = c.reduceTasks[i]
				go c.deferCheck(c.reduceTasks, v.Number, 10*time.Second)
				return nil
			}
		}

	}

	reply.Code = CodeNoAvailableTask
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	index := args.Task.Number - 1
	var taskToChange *MRTask
	switch args.Task.Type {
	case MapType:
		taskToChange = &c.mapTasks[index]
	case ReduceType:
		taskToChange = &c.reduceTasks[index]
	default:
		return errors.New("Unknown task type.")
	}

	c.lock.Lock()
	taskToChange.Status = StatusFinished
	c.finished++
	c.lock.Unlock()

	reply.Code = CodeOk
	return nil
}

func (c *Coordinator) FailedTask(args *FailedTaskArgs, reply *FailedTaskReply) error {
	fmt.Printf("Worker %s-%d failed. The Reason is: %s", ConvertTaskTypeToString(args.Task.Type), args.Task.Number, args.Reason)

	index := args.Task.Number - 1
	var taskToChange *MRTask
	switch args.Task.Type {
	case MapType:
		taskToChange = &c.mapTasks[index]
	case ReduceType:
		taskToChange = &c.reduceTasks[index]
	default:
		return errors.New("Unknown task type.")
	}

	c.lock.Lock()
	taskToChange.Status = StatusNotStart
	c.lock.Unlock()

	return nil
}
