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
	nextToken   uint
	crashWorker []uint // record the crashed workers
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

	c.nextToken = 1

	c.server()
	return &c
}

func (c *Coordinator) deferCheck(task *MRTask, token uint, timeout time.Duration) {
	time.Sleep(timeout)
	c.lock.Lock()
	if task.Status != StatusFinished {
		task.Status = StatusNotStart
		c.crashWorker = append(c.crashWorker, token)
	}
	c.lock.Unlock()
}

func (c *Coordinator) assign(task *MRTask, reply *AskForTaskReply) {
	task.Status = StatusRunning
	reply.Code = CodeOk
	reply.Task = *task
	go c.deferCheck(task, c.nextToken, 10*time.Second)
	reply.Token = c.nextToken
	c.nextToken++
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	if c.Done() {
		reply = &AskForTaskReply{Code: CodeAllTasksFinished}
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	for _, token := range c.crashWorker {
		if args.Token == token {
			reply.Code = CodeWorkerCrash
			return nil
		}
	}

	// find avaible tasks
	reply.NMap = len(c.mapTasks)
	reply.NReduce = len(c.reduceTasks)
	allMapTasksFinished := true
	for i, v := range c.mapTasks {
		if v.Status == StatusNotStart {
			c.assign(&c.mapTasks[i], reply)
			return nil
		} else if v.Status == StatusRunning {
			allMapTasksFinished = false
		}
	}

	if allMapTasksFinished {
		for i, v := range c.reduceTasks {
			if v.Status == StatusNotStart {
				c.assign(&c.reduceTasks[i], reply)
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
