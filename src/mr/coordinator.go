package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
			Status: NotStart,
		}
	}

	c.reduceTasks = make([]MRTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = MRTask{
			Type:   ReduceType,
			Number: i + 1,
			File:   "",
			Status: NotStart,
		}
	}

	c.lock = sync.Mutex{}

	c.server()
	return &c
}

func (c *Coordinator) AskForTask(noArgs *interface{}, t *MRTask) error {
	if c.Done() {
		return AllTaskFinished
	}

	// find avaible tasks
	c.lock.Lock()
	for _, v := range c.mapTasks {
		if v.Status == NotStart {
			*t = v
			return nil
		}
	}

	for _, v := range c.reduceTasks {
		if v.Status == NotStart {
			*t = v
			return nil
		}
	}
	c.lock.Unlock()

	return NoTaskAvailable
}

func (c *Coordinator) FinishTask(task *MRTask, reply *bool) error {
	index := task.Number - 1
	var taskToChange *MRTask
	switch task.Type {
	case MapType:
		taskToChange = &c.mapTasks[index]
	case ReduceType:
		taskToChange = &c.reduceTasks[index]
	default:
		return errors.New("Unknown task type.")
	}

	c.lock.Lock()
	taskToChange.Status = Finished
	c.finished++
	c.lock.Unlock()

	return nil
}
