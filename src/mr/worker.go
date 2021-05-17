package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList struct {
	Key    string
	Values []string
}

type workerContext struct {
	currentTask MRTask
	nReduce     int
	nMap        int
	mapf        func(string, string) []KeyValue
	reducef     func(string, []string) string
	token       uint // The unique key for a worker, that will be changed after asking a atsk
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	ctx := workerContext{
		mapf:    mapf,
		reducef: reducef,
		token:   0,
	}

	running := true
	for running {
		var reply AskForTaskReply
		if !call("Coordinator.AskForTask", &AskForTaskArgs{Token: ctx.token}, &reply) {
			fmt.Printf("Cannot call the coordinator. The coordinator may finish.\n")
			break
		}

		switch reply.Code {
		case CodeOk:
			ctx.currentTask = reply.Task
			ctx.nReduce = reply.NReduce
			ctx.nMap = reply.NMap
			ctx.token = reply.Token
			err := execTask(&ctx)
			if err != nil {
				fmt.Print(err)
				if !call(
					"Coordinator.FailedTask",
					&FailedTaskArgs{
						Reason: err.Error(),
						Task:   ctx.currentTask,
					},
					&FinishTaskReply{}) {
					running = false
				} else {
					time.Sleep(2 * time.Second)
					continue
				}
			}

			// Call FinishTask
			var fReply FinishTaskReply
			if !call(
				"Coordinator.FinishTask",
				&FinishTaskArgs{
					Task: ctx.currentTask,
				},
				&fReply) {
				// What should the worker do?
				// I am confused
				running = false
			}

		case CodeAllTasksFinished:
			fmt.Printf("All tasks finished.")
			running = false

		case CodeNoAvailableTask:
			// No operation

		case CodeWorkerCrash:
			// No operation
		}

		time.Sleep(2 * time.Second)
	}

	fmt.Printf("The worker returns.\n")
}

func execTask(ctx *workerContext) error {
	var err error
	switch ctx.currentTask.Type {
	case MapType:
		err = execMapTask(ctx)
	case ReduceType:
		err = execReduceTask(ctx)
	}
	if err != nil {
		return err
	}

	return nil
}

func execMapTask(ctx *workerContext) error {
	kv, err := inputFromFile(ctx.currentTask.File)
	if err != nil {
		return nil
	}

	err = outputToIntermediate(ctx, ctx.mapf(kv.Key, kv.Value))
	return nil
}

func execReduceTask(ctx *workerContext) error {
	kva, err := inputFromIntermediate(ctx)
	if err != nil {
		return nil
	}
	sort.Sort(ByKey(kva))

	err = outputToResults(ctx, kva)
	if err != nil {
		return err
	}

	return nil
}

func inputFromFile(filename string) (kv KeyValue, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return kv, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return kv, err
	}

	kv.Key = filename
	kv.Value = string(content)
	return kv, nil
}

func outputToIntermediate(ctx *workerContext, kva []KeyValue) (err error) {
	baseOname := "mr-" + strconv.Itoa(ctx.currentTask.Number) + "-"
	kvBuckets := make([][]KeyValue, ctx.nReduce)

	for _, kv := range kva {
		bIndex := ihash(kv.Key) % ctx.nReduce
		kvBuckets[bIndex] = append(kvBuckets[bIndex], kv)
	}

	for bIndex := 0; bIndex < ctx.nReduce; bIndex++ {
		oname := baseOname + strconv.Itoa(bIndex+1)
		ofile, err := os.Create(oname)
		if err != nil {
			return err
		}

		encoder := json.NewEncoder(ofile)
		kvBucket := kvBuckets[bIndex]
		for _, kv := range kvBucket {
			encoder.Encode(&kv)
		}
		ofile.Close()
	}

	return nil
}

func inputFromIntermediate(ctx *workerContext) (kva []KeyValue, err error) {
	for i := 0; i < ctx.nMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i+1, ctx.currentTask.Number)
		ifile, err := os.Open(iname)
		if err != nil {
			return kva, err
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return kva, nil
}

func outputToResults(ctx *workerContext, kva []KeyValue) (err error) {
	// Create the output file
	oname := "mr-out-" + strconv.Itoa(ctx.currentTask.Number) // mr-out-${reduceNumber}
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	// For each key
	i := 0
	for i < len(kva) {
		// Merge the same keys
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := KeyValue{Key: kva[i].Key, Value: ctx.reducef(kva[i].Key, values)}

		// Write
		fmt.Fprintf(ofile, "%v %v\n", output.Key, output.Value)

		i = j
	}

	return nil
}

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
