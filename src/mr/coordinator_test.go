package mr

import (
	"testing"
	"time"
)

func TestCoordinator_AskForTask(t *testing.T) {
	files := []string{"pg-being_ernest.txt", "pg-grimm.txt", "pg-dorian_gray.txt"}
	m := MakeCoordinator(files, 10)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if m.Done() {
				break
			}
		}
	}()

	time.Sleep(2 * time.Second)
	replies := make([]AskForTaskReply, 3)
	for i := 0; i < 3; i++ {
		call("Coordinator.AskForTask", &AskForTaskArgs{}, &replies[i])
		if replies[i].NReduce != 10 {
			t.Fail()
		}
		if replies[i].Task.Type != MapType {
			t.Fail()
		}
		if replies[i].Task.Number != i+1 {
			t.Fail()
		}
	}

}
