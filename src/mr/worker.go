package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

func init()  {
	outfile, _ := os.OpenFile("master.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	log.SetOutput(outfile)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NTotal to choose the reduce
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

func doTaskTrace(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,task Task,info *TaskInfo) {
	switch task.Type {
	case Map: {
		doMapTask(mapf,task,info)
	}
	case Reduce: {
		doReduceTask(reducef,task,info)
	}
	}
}

func doMapTask(mapf func(string, string) []KeyValue,task Task,info *TaskInfo) {
	log.Println(os.Getpid(),TaskTypeName[task.Type],task.Id,"Task Doing" )
	//fmt.Println(info.MapDataPath)
	for _,filename := range []string{info.MapDataPath[task.Id]} {
		mapInputFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(mapInputFile)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		mapInputFile.Close()

		kva := mapf(filename, string(content))
		jsonec := make([]*json.Encoder,info.TaskNum[Reduce])
		files := make([]*os.File,info.TaskNum[Reduce])
		for i:=0;i<info.TaskNum[Reduce];i++ {
			mapOutputFile,err:=os.Create(fmt.Sprintf("mr-%v-%v",task.Id,i))
			if err != nil {
				log.Fatalf("cannot create %v", fmt.Sprintf("mr-%v-%v", task.Id, i))
			}
			files[i] = mapOutputFile
			jsonec[i] = json.NewEncoder(mapOutputFile)
		}

		for _,kv := range kva {
			//fmt.Println(fmt.Sprintf("mr-%v-%v",reply.CurTaskId,ihash(kv.Key)%reply.NTotal))
			err = jsonec[ihash(kv.Key)%info.TaskNum[Reduce]].Encode(&kv)
			if err!= nil {
				log.Println(err)
			}
		}
		for i:=0;i<info.TaskNum[Reduce];i++ {
			files[i].Close()
		}
	}
	var tmp string
	call("Master.CurTaskDone",&task,&tmp)
}

func doReduceTask(reducef func(string, []string) string,task Task,info *TaskInfo) {
	log.Println(os.Getpid(),TaskTypeName[task.Type],task.Id,"Task Doing" )
	intermediate := []KeyValue{}

	for i:=0;i<info.TaskNum[Map];i++ {
		reduceInputFile,err:=os.Open(fmt.Sprintf("mr-%v-%v", i, task.Id))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("mr-%v-%v", i, task.Id))
		}
		dec := json.NewDecoder(reduceInputFile)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			} else {
				intermediate = append(intermediate, kv)
			}
		}
		reduceInputFile.Close()
	}

	reduceOutputFile, err := os.Create(fmt.Sprintf("mr-out-%v", task.Id))
	if err != nil {
		log.Fatalf("cannot create %v", fmt.Sprintf("mr-out-%v", task.Id))
	}
	sort.Sort(ByKey(intermediate))
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
		fmt.Fprintf(reduceOutputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	reduceOutputFile.Close()
	var tmp string
	call("Master.CurTaskDone",&task,&tmp)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	var taskInfo TaskInfo
	call("Master.GetTaskInfo","",&taskInfo)
	for i:=0;i<TypeNum;i++ {
		reply := []bool{false,false}
		call("Master.GetCurState","",&reply)
		for reply[i] == false {
			task := Task{}
			if call("Master.GetTask",i,&task) == false {
				return
			}
			log.Println(os.Getpid(),TaskTypeName[task.Type],task.Id,"Task Get" )
			doTaskTrace(mapf,reducef,task,&taskInfo)
			call("Master.GetCurState","",&reply)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
