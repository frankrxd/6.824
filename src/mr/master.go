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
	workers             []WorkerInfo
	maptasks            []MapTaskInfo
	reducetasks         []ShuffleTidyTaskInfo
	DoneMapChan         []chan bool
	DoneReduceChan      []chan bool
	MapChan             chan int
	ReduceChan          chan int
	DoneTotalMapChan    chan bool
	DoneTotalReduceChan chan bool
	FinishedMap         map[int]bool
	FinishedReduce      map[int]bool
	nMap                int
	nReduce             int
	mutexMap			sync.Mutex
	mutexReduce			sync.Mutex
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
	log.Println("ReduceProduce task :", taskid)
	m.ReduceChan <- taskid
}

func (m *Master) MapConsumer(taskid *int) bool{
	var ok bool
	*taskid, ok = <-m.MapChan
	if ok == false {
		return false
	}
	log.Println("MapConsume task : ", *taskid)
	go func() {
		for {
			select {
			case <-m.DoneMapChan[*taskid]:
				{
					log.Println("MapTask has done :", *taskid)
					return
				}
			case <-time.After(10 * time.Second):
				{
					log.Println("MapTask timeout :", *taskid)
					m.MapProducer(*taskid)
					return
					//将此maptask加到MapProduce中
				}
			}
		}
	}()
	return true
}

func (m *Master) ReduceConsumer(taskid *int) bool{
	var ok bool
	*taskid, ok = <-m.ReduceChan
	if ok == false {
		return false
	}
	log.Println("ReduceConsume task :", *taskid)
	go func() {
		for {
			select {
			case <-m.DoneReduceChan[*taskid]:
				{
					log.Println("ReduceTask has done :", *taskid)
					close(m.DoneReduceChan[*taskid])
					return
				}
			case <-time.After(10 * time.Second):
				{
					log.Println("ReduceTask timeout :", *taskid)
					m.ReduceProducer(*taskid)
					return
					//将此maptask加到MapProduce中
				}
			}
		}
	}()
	return true
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

func (m *Master) CurReduceTaskDone(args *int, reply *string) error {
	m.DoneReduceChan[*args] <- true
	m.mutexReduce.Lock()
	m.FinishedReduce[*args] = true
	if len(m.FinishedReduce) == m.nReduce {
		m.DoneTotalReduceChan <- true
	}
	m.mutexReduce.Unlock()
	return nil
}

func (m *Master) CurMapTaskDone(args *int, reply *string) error {
	m.DoneMapChan[*args] <- true
	m.mutexMap.Lock()
	m.FinishedMap[*args] = true
	if len(m.FinishedMap) == m.nMap {
		m.DoneTotalMapChan <- true
	}
	m.mutexMap.Unlock()
	return nil
}

func (m *Master) GetReduceTask(args *string, reply *TaskReply) error {
	mutex := sync.Mutex{}
	//log.Println("rpc GetReduceTask start")
	var taskid int
	if m.ReduceConsumer(&taskid) == false {
		return errors.New("ReduceChan has closed")
	}
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
	if m.MapConsumer(&taskid) == false {
		return errors.New("MapChan has closed")
	}
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
	MapTaskFinished    bool
	ReduceTaskFinished bool
}

func (m *Master) GetCurState(args *string, reply *StateReply) error {
	mutex := sync.Mutex{}
	mutex.Lock()
	if len(m.FinishedMap) < m.nMap {
		(*reply).MapTaskFinished = false
	} else {
		(*reply).MapTaskFinished = true
	}
	if len(m.FinishedReduce) < m.nReduce {
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
	m := Master{}
	m.nReduce = nReduce
	m.nMap = len(os.Args[1:])
	m.MapChan = make(chan int,m.nMap)
	m.ReduceChan = make(chan int,m.nReduce)
	m.DoneTotalMapChan = make(chan bool)
	m.DoneTotalReduceChan = make(chan bool)
	m.FinishedMap = make(map[int]bool)
	m.FinishedReduce = make(map[int]bool)

	// Your code here.
	// init m.maptasks // init m.DoneMapChan

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
