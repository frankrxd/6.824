package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map  =  0
	Reduce  = 1
	TypeNum = 2
)
var TaskTypeName = []string{"Map","Reduce"}


type Master struct {
	// Your definitions here.
	TaskNum				[]int
	MapDataPath			[]string
	ReduceDataPath		[][]string
	TaskChan			[]chan Task
	TaskDoneChan		[][]chan struct{}
	TaskFinishedChan	[]chan struct{}
	FinishedTask        []map[int]struct{}
	TaskFinished		[]bool
	mutex				[]sync.Mutex
	mutexReduce			sync.Mutex
}


type Task struct {
	Id		int
	Type	int
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type TaskReply struct {
	Task		Task
}


func (m *Master) Producer(task Task) {
	log.Println("Produce task:", TaskTypeName[task.Type],task.Id)
	m.TaskChan[task.Type] <- task
}

func (m *Master) Consumer(tasktype int) (Task,bool) {
	task,ok := <-m.TaskChan[tasktype]
	if ok == false {
		return Task{},false
	}
	log.Println("Consume task : ",TaskTypeName[task.Type],task.Id)
	go func() {
		select {
		case <-m.TaskDoneChan[task.Type][task.Id]:
			{
				log.Println("Task has done :", TaskTypeName[task.Type],task.Id)
				close(m.TaskDoneChan[task.Type][task.Id])
				return
			}
		case <-time.After(10 * time.Second):
			{
				log.Println("Task timeout :", TaskTypeName[task.Type],task.Id)
				m.Producer(task)
				return
				//将此task加到Produce中
			}
		}
	}()
	return task,true

}

func (m *Master) ProduceReduceTask() {
	go func() {
		<-m.DoneTotalMapChan //Map任务完成
		close(m.DoneTotalMapChan)
		close(m.MapChan)
		log.Println("Map task has finished!")
		for i := 0; i < m.nReduce; i++ {
			go m.ReduceProducer(i)
		}
	}()
}

func (m *Master)GetTask(tasktypes *int, reply *Task) error {
	task,ok := m.Consumer(*tasktypes)
	if ok != false {
		*reply = task
	} else {
		return errors.New("GetTask: get task failed (chan is closed)")
	}
	return nil
}

func (m *Master) CurTaskDone(task *Task, reply *string) error {
	m.TaskDoneChan[task.Type][task.Id] <- struct{}{}
	m.mutex[task.Type].Lock()
	m.FinishedTask[task.Type][task.Id] = struct{}{}
	if len(m.FinishedTask[task.Type]) == m.TaskNum[task.Type] {
		m.TaskFinishedChan[task.Type] <- struct{}{}
		m.TaskFinished[task.Type] = true
	}
	m.mutex[task.Type].Unlock()
	return nil
}

type StateReply struct {
	state []bool
}
func (m *Master) GetCurState(args *string, reply *StateReply) error {
	mutex := sync.Mutex{}master.go
		mutex.Lock()
	(*reply).state = m.TaskFinished
	mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	log.Printf("start listen %s\n", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	<-m.DoneTotalReduceChan //Reduce任务完成
	close(m.DoneTotalReduceChan)
	close(m.ReduceChan)
	ret = true
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(os.Args[1:])
	m := Master {
		TaskNum:			[]int{nMap,nReduce},
		MapDataPath:		os.Args[1:],
		ReduceDataPath:		make([][]string,nReduce),
		TaskChan:			make([]chan Task,TypeNum),
		TaskDoneChan:		make([][]chan struct{},TypeNum),
		TaskFinishedChan:	make([]chan struct{},TypeNum),
		FinishedTask:       make([]map[int]struct{},TypeNum),
		TaskFinished:		[]bool{false,false},
		mutex:				make([]sync.Mutex,TypeNum),
	}
	for i := 0; i < TypeNum; i++ {
		m.TaskDoneChan[i] = make([]chan struct{},m.TaskNum[i])
		for j := 0; j < m.TaskNum[i]; j++ {
			m.Producer(Task{j,i})
		}
	}

	for i := 0; i < nReduce; i++ {
		Path := []string{}
		for j := 0; j < m.nMap; j++ {
			Path = append(Path, fmt.Sprintf("mr-%v-%v", j, i))
		}
		m.reducetasks = append(m.reducetasks, ShuffleTidyTaskInfo{i, Path})
	}


	m.server()
	m.ProduceReduceTask()
	return &m
}
