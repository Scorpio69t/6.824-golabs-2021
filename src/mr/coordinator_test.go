package mr

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func runCoordinator(nMap int, nReduce int) (c *Coordinator) {

	files := []string{"pg-being_ernest.txt", "pg-grimm.txt", "pg-dorian_gray.txt", "pg-grimm.txt", "pg-tom_sawyer.txt"}
	c = MakeCoordinator(files[:nMap], nReduce)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if c.Done() {
				break
			}
		}
	}()
	time.Sleep(1 * time.Second)

	return c
}

func TestCoordinator_AskForTask(t *testing.T) {
	nMap := 3
	nReduce := 10
	c := runCoordinator(nMap, nReduce)

	for i := 0; i < nMap; i++ {
		var askForTaskReply AskForTaskReply
		call("Coordinator.AskForTask", &AskForTaskArgs{}, &askForTaskReply)
		if askForTaskReply.Code != CodeOk {
			t.Fail()
		}
		if askForTaskReply.NReduce != nReduce || askForTaskReply.NMap != nMap {
			t.Fail()
		}
	}

	for i := 0; i < nReduce; i++ {
		var askForTaskReply AskForTaskReply
		call("Coordinator.AskForTask", &AskForTaskArgs{}, &askForTaskReply)
		if askForTaskReply.Code != CodeNoAvailableTask {
			t.Fail()
		}
		if askForTaskReply.NReduce != nReduce || askForTaskReply.NMap != nMap {
			t.Fail()
		}
	}

	fmt.Print(c)
}

func TestCoordinator_FinishTask(t *testing.T) {
	nMap := 3
	nReduce := 3
	c := runCoordinator(nMap, nReduce)

	// Ask for map task and finish it
	for i := 0; i < nMap; i++ {
		var askForTaskReply AskForTaskReply
		call("Coordinator.AskForTask", &AskForTaskArgs{}, &askForTaskReply)
		if askForTaskReply.Code != CodeOk {
			t.Fail()
		}
		if askForTaskReply.NReduce != nReduce || askForTaskReply.NMap != nMap {
			t.Fail()
		}

		var finishTaskReply FinishTaskReply
		call("Coordinator.FinishTask", &FinishTaskArgs{Task: askForTaskReply.Task}, &finishTaskReply)
	}

	// Ask for reduce task and finish it
	for i := 0; i < nReduce; i++ {
		var askForTaskReply AskForTaskReply
		call("Coordinator.AskForTask", &AskForTaskArgs{}, &askForTaskReply)
		if askForTaskReply.Code != CodeOk {
			t.Fail()
		}
		if askForTaskReply.NReduce != nReduce || askForTaskReply.NMap != nMap {
			t.Fail()
		}

		var finishTaskReply FinishTaskReply
		call("Coordinator.FinishTask", &FinishTaskArgs{Task: askForTaskReply.Task}, &finishTaskReply)
	}

	if c.finished != nMap+nReduce {
		t.Fail()
	}
	for _, task := range c.mapTasks {
		if task.Status != StatusFinished {
			t.Fail()
		}
	}
	for _, task := range c.reduceTasks {
		if task.Status != StatusFinished {
			t.Fail()
		}
	}

	fmt.Print(c)
}

func TestCoordinator_deferCheck(t *testing.T) {
	nMap := 3
	nReduce := 10
	c := runCoordinator(nMap, nReduce)
	waitGroup := sync.WaitGroup{}

	for i := 0; i < nMap; i++ {
		var askForTaskReply AskForTaskReply
		call("Coordinator.AskForTask", &AskForTaskArgs{Token: 0}, &askForTaskReply)
		if askForTaskReply.Code != CodeOk {
			t.Fail()
		}
		if askForTaskReply.NReduce != nReduce || askForTaskReply.NMap != nMap {
			t.Fail()
		}
		waitGroup.Add(1)
		go func(wg *sync.WaitGroup, token uint) {
			time.Sleep(10*time.Second + 100*time.Millisecond)
			var askForTaskReply2 AskForTaskReply
			call("Coordinator.AskForTask", &AskForTaskArgs{Token: token}, &askForTaskReply2)
			if askForTaskReply2.Code != CodeWorkerCrash {
				t.Fail()
			}
			wg.Done()
		}(&waitGroup, askForTaskReply.Token)
	}

	waitGroup.Wait()

	if len(c.crashWorker) != nMap {
		t.Fail()
	}

	fmt.Print(c)
}
