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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// this means the task type of the worker
const (
	MapTask int = iota
	ReduceTask
	WaitingTask
	ExitTask
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// TaskArgs is no use, just for pasa parm to call func
type TaskArgs struct{}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	isexit := true
	for isexit {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				StartMapTask(&task, mapf)
				SetTaskDone(&task)
			}
		case ReduceTask:
			{
				StartReduceTask(&task, reducef)
				SetTaskDone(&task)
			}
		case WaitingTask:
			{
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				isexit = false
			}
		}
	}
}

// worker get task from coordinator
func GetTask() TaskInfo {
	args := TaskArgs{}
	reply := TaskInfo{}
	ok := call("Coordinator.PublishTask", &args, &reply)
	if ok {
		//fmt.Println("call success,the TaskId is : ", reply.TaskId)
	} else {
		fmt.Println("call failed!")
	}
	return reply
}

// worker set task state form working->done
func SetTaskDone(task *TaskInfo) {
	args := task
	reply := ExampleReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		fmt.Println("task done call fail!")
	}
}

// begin Map task
func StartMapTask(task *TaskInfo, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName[0])
	}
	file.Close()
	intermediate = mapf(task.FileName[0], string(content))
	Hashkv := make([][]KeyValue, task.NReduce)
	for _, v := range intermediate {
		index := ihash(v.Key) % task.NReduce
		Hashkv[index] = append(Hashkv[index], v)
	}
	for i := 0; i < task.NReduce; i++ {
		tmpfile := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(tmpfile)
		if err != nil {
			log.Fatal("create file failed: ", err)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range Hashkv[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("encode failed ", err)
			}
		}
		ofile.Close()
	}
}

// begin Reduce task
func StartReduceTask(task *TaskInfo, reducef func(string, []string) string) {
	intermediate := FileSort(task.FileName)
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, err := os.Create(outputFile)
	if err != nil {
		log.Fatal("Reduce task can not create file", err)
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
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

func FileSort(files []string) []KeyValue {
	res := []KeyValue{}
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatal("Open File %v Fail", f)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			res = append(res, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(res))
	return res
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
