package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.RWMutex

// task state
const (
	Waiting int = iota
	Working
	Finish
)

// coordinate parse
const (
	MapParse int = iota
	ReduceParse
	AllDone
)

// hold the state of the task
type TaskMetaInfo struct {
	Task      *TaskInfo
	StartTime time.Time
	TaskState int
}

// using a map to hold the info of task state:
// MetaMap[TaskID]->TaskMetaInfo
type MetaInfoMap struct {
	MetaMap map[int]*TaskMetaInfo
}

type Coordinator struct {
	// Your definitions here.
	State      int
	Files      []string
	MapChan    chan *TaskInfo
	ReduceChan chan *TaskInfo
	NReduce    int
	MetaInfo   MetaInfoMap
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) getState() int {
	mu.RLock()
	defer mu.RUnlock()
	return c.State
}

// the RPC calls the coordinate func PublishTask when worker ask for a task
func (c *Coordinator) PublishTask(args *TaskArgs, reply *TaskInfo) error {
	//fmt.Println("Coordinate get a distribution request from worker")
	//if c.State == MapParse {
	if c.getState() == MapParse { //use getState to aviod data race
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			if !c.MetaInfo.recordTaskStart(reply.TaskId) {
				fmt.Printf("Task %d is running or has done !Fail to start task", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.MetaInfo.checkAllTaskDone(c.getState()) { //use getState to aviod data race
				c.toNextParse()
			}
			return nil
		}
	} else if c.getState() == ReduceParse {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			if !c.MetaInfo.recordTaskStart(reply.TaskId) {
				fmt.Printf("Task %d is running or has done !Fail to start task", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask
			if c.MetaInfo.checkAllTaskDone(c.getState()) {
				c.toNextParse()
			}
		}
		return nil
	} else {
		reply.TaskType = ExitTask
	}
	return nil
}

// Worker pass task done rpc to Coordinate
func (c *Coordinator) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.MetaInfo.MetaMap[args.TaskId]
	if ok && meta.TaskState == Working {
		meta.TaskState = Finish
		//fmt.Printf("Task %d Done!\n", args.TaskId)
	} else {
		fmt.Printf("Task %d has already Done!\n", args.TaskId)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.RLock()
	ret = (c.State == AllDone)
	mu.RUnlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:      MapParse,
		Files:      files,
		MapChan:    make(chan *TaskInfo, len(files)),
		ReduceChan: make(chan *TaskInfo, nReduce),
		NReduce:    nReduce,
		MetaInfo:   MetaInfoMap{make(map[int]*TaskMetaInfo)},
	}
	// Your code here.
	c.MakeMapTask(files)
	//c.MakeReduceTask()
	c.server()
	return &c
}

// put the task into MetaInfoMap
func (m *MetaInfoMap) putTaskInMap(tinfo *TaskMetaInfo) bool {
	taskid := tinfo.Task.TaskId
	_, ok := m.MetaMap[taskid]
	if ok {
		fmt.Println("MetaInfoMap has already contains the Task:", taskid)
		return false
	}
	m.MetaMap[taskid] = tinfo
	return true
}

// After (1) put the task into MetaInfoMap (2) put the task into channel
// next, we change the TaskState and record the starttime
func (m *MetaInfoMap) recordTaskStart(taskid int) bool {
	mu.Lock()
	defer mu.Unlock()
	taskinfo, ok := m.MetaMap[taskid]
	if !ok || taskinfo.TaskState != Waiting {
		return false
	}
	m.MetaMap[taskid].StartTime = time.Now()
	m.MetaMap[taskid].TaskState = Working
	return true
}

// generate map task
func (c *Coordinator) MakeMapTask(files []string) {
	for id, v := range files {
		task := TaskInfo{
			TaskType: MapTask,
			TaskId:   id,
			FileName: []string{v},
			NReduce:  c.NReduce,
		}
		taskmetainfo := TaskMetaInfo{
			Task:      &task,
			TaskState: Waiting,
		}
		c.MetaInfo.putTaskInMap(&taskmetainfo)
		c.MapChan <- &task
		//fmt.Println("file", v, "写入Map channel成功!")
	}
}

// generate reduce task
func (c *Coordinator) MakeReduceTask() {
	for i := 0; i < c.NReduce; i++ {
		task := TaskInfo{
			TaskType: ReduceTask,
			TaskId:   i + len(c.Files),
			FileName: findReduceFiles(i),
			NReduce:  c.NReduce,
		}
		taskmetainfo := TaskMetaInfo{
			Task:      &task,
			TaskState: Waiting,
		}
		c.MetaInfo.putTaskInMap(&taskmetainfo)
		c.ReduceChan <- &task
		//fmt.Println("Task", i+len(c.Files), "写入Reduce channel成功!相关文件为：", findReduceFiles(i))
	}
}

// select the target intermediate file from temp files mr-tmp-x-y
// y is the reduce file id
func findReduceFiles(i int) []string {
	res := []string{}
	workName, _ := os.Getwd()
	files, _ := ioutil.ReadDir(workName)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-tmp-") && strings.HasSuffix(f.Name(), strconv.Itoa(i)) {
			res = append(res, f.Name())
		}
	}
	return res
}

// check if the current coordinate parse is done
func (m *MetaInfoMap) checkAllTaskDone(state int) bool {
	doneNum, unDoneNum := 0, 0
	mu.RLock()
	defer mu.RUnlock()
	if state == MapParse {
		for _, v := range m.MetaMap {
			if v.TaskState == Finish {
				doneNum++
			} else {
				unDoneNum++
			}
		}
	} else if state == ReduceParse {
		for _, v := range m.MetaMap {
			if v.TaskState == Finish {
				doneNum++
			} else {
				unDoneNum++
			}
		}
	}
	return (unDoneNum == 0 && doneNum > 0)
}

// change the parse of the coordinate
// map->reduce reduce->alldone
func (c *Coordinator) toNextParse() {
	mu.Lock()
	defer mu.Unlock()
	if c.State == MapParse {
		c.MakeReduceTask()
		c.State = ReduceParse
	} else if c.State == ReduceTask {
		c.State = AllDone
	}
}

// Check if the worker do the task > 10s
// if true,stop the worker,put the task into channel again
func (c *Coordinator) CheckTimeOut() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.State == AllDone {
			mu.Unlock()
			break
		}
		for _, meta := range c.MetaInfo.MetaMap {
			if meta.TaskState == Working && time.Since(meta.StartTime) > 10*time.Second {
				if meta.Task.TaskType == MapTask {
					meta.TaskState = Waiting
					c.MapChan <- meta.Task
				} else if meta.Task.TaskType == ReduceTask {
					meta.TaskState = Waiting
					c.ReduceChan <- meta.Task
				}
			}
		}
		mu.Unlock()
	}
}
