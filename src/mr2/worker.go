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
func doMapTask(mapf func(string, string) []KeyValue) {
	var reply TaskReply
	if call("Master.GetMapTask","",&reply) == false {
		return
	}
	log.Println("doMapTask :",reply.CurTaskId)
	for _,filename := range reply.Path {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply)
		}
		file.Close()

		kva := mapf(filename, string(content))
		var jsonec []*json.Encoder
		var files []*os.File
		for i:=0;i<reply.NTotal;i++ {
			file,_:=os.OpenFile(fmt.Sprintf("mr-%v-%v",reply.CurTaskId,i),os.O_RDWR|os.O_CREATE, 0755)

			files = append(files,file)
			jsonec = append(jsonec,json.NewEncoder(file))
		}

		for _,kv := range kva {
			//fmt.Println(fmt.Sprintf("mr-%v-%v",reply.CurTaskId,ihash(kv.Key)%reply.NTotal))
			jsonec[ihash(kv.Key)%reply.NTotal].Encode(&kv)
		}
		for i:=0;i<reply.NTotal;i++ {
			//fmt.Println(files[i].Name())
			files[i].Close()
		}
	}
	var tmp string
	call("Master.CurMapTaskDone",&reply.CurTaskId,&tmp)
}


func doReduceTask(reducef func(string, []string) string) {
	var reply TaskReply
	if call("Master.GetReduceTask","",&reply) == false {
		return
	}
	log.Println("doReduceTask :",reply.CurTaskId)
	intermediate := []KeyValue{}
	for _, filename := range reply.Path {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", reply.CurTaskId))
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
	var tmp string
	call("Master.CurReduceTaskDone",&reply.CurTaskId,&tmp)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	reply := StateReply{false,false}
	call("Master.GetCurState","",&reply)
	for reply.MapTaskFinished == false {
		doMapTask(mapf)
		call("Master.GetCurState","",&reply)
	}
	call("Master.GetCurState","",&reply)
	for reply.ReduceTaskFinished == false {
		doReduceTask(reducef)
		call("Master.GetCurState","",&reply)
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

	fmt.Println(err)
	return false
}
