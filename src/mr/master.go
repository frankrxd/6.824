package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStateType int
type WorkerStateType int



const (
	TASK_MAP_FINISHED  TaskStateType = 0
	TASK_MAP_FAILD     TaskStateType = -1
	TASK_MAP_UNTREATED TaskStateType = 1
	TASK_MAP_DOING     TaskStateType = 2

	TASK_REDUCE_FINISHED TaskStateType = 0
	TASK_REDUCE_FAILD    TaskStateType = -1
	TASK_REDUCE_PREPARED TaskStateType = 1
	TASK_REDUCE_PENDING  TaskStateType = 3
	TASK_REDUCE_DOING    TaskStateType = 2
)
const (
	WORKERUNOCCUPIED WorkerStateType = 1
	WORKERDOING      WorkerStateType = 0
	WORKERFAILED     WorkerStateType = -1
)

type Master struct {
	// Your definitions here.
	workers              []WorkerInfo
	maptasks             []MapTaskInfo
	reducetasks          []ShuffleTidyTaskInfo
	DoneMapChan          []chan bool
	DoneReduceChan       []chan bool
	MapChan              chan int
	ReduceChan           chan int
	DoneTotalMapChan     chan bool
	DoneTotalReduceChan  chan bool
	TotalDoneMapCount    int32
	TotalDoneReduceCount int32
	nMap                 int
	nReduce              int
}

type WorkerInfo struct {
	State WorkerStateType
}

type MapTaskInfo struct {
	State TaskStateType
	Id    int
	Path  []string
}
type ShuffleTidyTaskInfo struct {
	State TaskStateType
	Id    int
	Path  []string
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
	CurTaskId int
	State     bool
	Path      []string
	NTotal    int
}

func (m *Master) MapProducer(taskid int) {
	log.Println("MapProduce task : ", taskid)
	m.MapChan <- taskid
}

func (m *Master) ReduceProducer(taskid int) {
	log.Println("ReduceProduce task : ", taskid)
	m.ReduceChan <- taskid
}

func (m *Master) MapConsumer(taskid *int) {
	*taskid = <-m.MapChan
	log.Println("MapConsume task : ", *taskid)
	go func() {
		for {
			select {
			case <-m.DoneMapChan[*taskid]:
				{
					log.Printf("Map Task %v has done!", *taskid)
					close(m.DoneMapChan[*taskid])
					return
				}
			case <-time.After(10 * time.Second):
				{
					log.Printf("Map Task %v timeout!", *taskid)
					m.MapProducer(*taskid)
					return
					//将此maptask加到MapProduce中
				}
			}
		}
	}()
}

func (m *Master) ReduceConsumer(taskid *int) {
	*taskid = <-m.ReduceChan
	log.Println("ReduceConsume task :", *taskid)
	go func() {
		for {
			select {
			case <-m.DoneReduceChan[*taskid]:
				{
					log.Printf("Reduce Task %v has done!", *taskid)
					close(m.DoneReduceChan[*taskid])
					return
				}
			case <-time.After(10 * time.Second):
				{
					log.Printf("Reduce Task %v timeout!", *taskid)
					m.ReduceProducer(*taskid)
					return
					//将此maptask加到MapProduce中
				}
			}
		}
	}()
}

func (m *Master) ProduceReduceTask() {
	go func() {
		<-m.DoneTotalMapChan //Map任务完成
		log.Println("Map task has finished!")
		for i := 0; i < m.nReduce; i++ {
			go m.ReduceProducer(i)
		}
	}()
}

func (m *Master) CurReduceTaskDone(args *int, reply *string) error {
	m.DoneReduceChan[*args] <- true
	m.TotalDoneReduceCount = atomic.AddInt32(&m.TotalDoneReduceCount, 1)
	if int(m.TotalDoneReduceCount) == m.nReduce {
		m.DoneTotalReduceChan <- true
	}
	return nil
}

func (m *Master) CurMapTaskDone(args *int, reply *string) error {
	m.DoneMapChan[*args] <- true
	m.TotalDoneMapCount = atomic.AddInt32(&m.TotalDoneMapCount, 1)
	//log.Println("TotalDoneMapCount :",m.TotalDoneMapCount)
	if int(m.TotalDoneMapCount) == m.nMap {
		m.DoneTotalMapChan <- true
	}
	return nil
}

func (m *Master) GetReduceTask(args *string, reply *TaskReply) error {
	mutex := sync.Mutex{}
	//log.Println("rpc GetReduceTask start")
	var taskid int = 0
	m.ReduceConsumer(&taskid)
	//log.Println("ReduceConsumer get taskid :", taskid)
	mutex.Lock()
	m.reducetasks[taskid].State = TASK_REDUCE_DOING
	mutex.Unlock()
	(*reply).NTotal = m.nMap
	(*reply).State = true
	(*reply).Path = m.reducetasks[taskid].Path
	(*reply).CurTaskId = m.reducetasks[taskid].Id
	//log.Println("rpc GetReduceTask return")
	return nil
}

func (m *Master) GetMapTask(args *string, reply *TaskReply) error {
	mutex := sync.Mutex{}
	//log.Println("rpc GetMapTask start")
	var taskid int = 0
	m.MapConsumer(&taskid)
	//log.Println("MapConsumer get taskid :", taskid)
	mutex.Lock()
	m.maptasks[taskid].State = TASK_MAP_DOING
	mutex.Unlock()
	(*reply).NTotal = m.nReduce
	(*reply).State = true
	(*reply).Path = m.maptasks[taskid].Path
	(*reply).CurTaskId = m.maptasks[taskid].Id
	//log.Println("rpc GetMapTask return")
	return nil
}

type StateReply struct {
	MapTaskFinished bool
	ReduceTaskFinished bool
}
func (m *Master) GetCurState(args *string, reply *StateReply) error {
	mutex := sync.Mutex{}
	mutex.Lock()
	if int(m.TotalDoneMapCount) < m.nMap {
		(*reply).MapTaskFinished = false
	} else {
		(*reply).MapTaskFinished = true
	}
	if int(m.TotalDoneReduceCount) < m.nReduce {
		(*reply).ReduceTaskFinished = false
	} else {
		(*reply).ReduceTaskFinished = true
	}
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
	<-m.DoneTotalReduceChan
	ret = true
	//select {
	//case <-m.DoneTotalReduceChan: {
	//	log.Println("Reduce task has finished!")
	//	close(m.DoneTotalReduceChan)
	//
	//}
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.MapChan = make(chan int)
	m.ReduceChan = make(chan int)
	m.DoneTotalMapChan = make(chan bool)
	m.DoneTotalReduceChan = make(chan bool)
	m.TotalDoneMapCount = 0
	m.TotalDoneReduceCount = 0
	// Your code here.
	// init m.maptasks // init m.DoneMapChan
	m.nMap = len(os.Args[1:])
	//log.Print(m.nMap)
	for i := 0; i < m.nMap; i++ {
		m.DoneMapChan = append(m.DoneMapChan, make(chan bool))
		m.maptasks = append(m.maptasks, MapTaskInfo{TASK_MAP_UNTREATED, i, []string{os.Args[1+i]}})
		go m.MapProducer(i)
	}
	//log.Print(m.maptasks)
	//log.Print("init m.maptasks finished!\n")

	//init m.reducetasks

	for i := 0; i < nReduce; i++ {
		m.DoneReduceChan = append(m.DoneReduceChan, make(chan bool))
		Path := []string{}
		for j := 0; j < m.nMap; j++ {
			Path = append(Path, fmt.Sprintf("mr-%v-%v", j, i))
		}
		m.reducetasks = append(m.reducetasks, ShuffleTidyTaskInfo{TASK_REDUCE_PENDING, i, Path})
	}
	//log.Print(m.reducetasks)
	//log.Print("init m.reducetasks finished!\n")
	m.server()
	m.ProduceReduceTask()
	return &m
}
