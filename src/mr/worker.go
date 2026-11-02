package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

var quit bool // indicates whether the worker should quit its work

func pendingTask() {
	time.Sleep(time.Second)
}

func mapTask(mapf func(string, string) []KeyValue, reply *RequestReply) {
	nReduce := reply.NReduce
	mapNum := reply.MapNum
	filename := reply.InputFilename

	finishPkt := MapFinishArgs{}
	finishPkt.Pid = os.Getpid()
	finishPkt.Number = mapNum

	finishReply := MapFinishReply{}

	file, err := os.Open(filename)
	if err != nil {
		// -1 indicates map task failed
		finishPkt.Number = -1*mapNum - 1
		call("Coordinator.MapFinish", &finishPkt, &finishReply)
		log.Fatalf("map task: cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		// -1 indicates map task failed
		finishPkt.Number = -1*mapNum - 1
		call("Coordinator.MapFinish", &finishPkt, &finishReply)
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// write KVA into intermediate files

	curWorkDir, _ := os.Getwd()

	tempFiles := make([]*os.File, nReduce)
	for i := range tempFiles {
		file, err := os.CreateTemp(curWorkDir, "mr-tmp-*")
		if err != nil {
			finishPkt.Number = -1*mapNum - 1
			call("Coordinator.MapFinish", &finishPkt, &finishReply)
			log.Fatal("CreateTemp():", err)
		}
		tempFiles[i] = file
	}

	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(tempFiles[i])
		enc.Encode(&kv)
	}

	{
		// debug
		// fmt.Println("os.Getwd():", curWorkDir)
	}

	for i, f := range tempFiles {
		f.Close()
		// intermediate file format: mr-X-Y
		realPath := curWorkDir + "/mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(i)

		err := os.Rename(f.Name(), realPath)
		if err != nil {
			log.Fatalf("map task: os.Rename error: %s", err)
		}
		finishPkt.TempFilenames = append(finishPkt.TempFilenames, realPath)
		{
			// debug
			// fmt.Println("map task: realPath:", realPath)
		}
	}

	ok := call("Coordinator.MapFinish", &finishPkt, &finishReply)
	if !ok {
		fmt.Println("Map Task failed!")
	}
}

func reduceTask(reducef func(string, []string) string, reply *RequestReply) {
	finishReply := ReduceFinishReply{}
	finishPkt := ReduceFinishArgs{}
	finishPkt.Pid = os.Getpid()
	finishPkt.Number = reply.ReduceNum

	filenames := reply.TempFilenames

	kva := []KeyValue{}
	for _, fn := range filenames {
		file, err := os.Open(fn)
		if err != nil {
			finishPkt.Number = -1*finishPkt.Number - 1
			call("Coordinator.ReduceFinish", &finishPkt, &finishReply)
			log.Fatalf("reduce task: cannot open %v", fn)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	curWorkDir, _ := os.Getwd()
	file, err := os.CreateTemp(curWorkDir, "mr-tmp-*")
	if err != nil {
		finishPkt.Number = -1*finishPkt.Number - 1
		call("Coordinator.ReduceFinish", &finishPkt, &finishReply)
		log.Fatal("CreateTemp():", err)
	}

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-Y.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	file.Close()

	realPath := curWorkDir + "/mr-out-" + strconv.Itoa(finishPkt.Number)
	err = os.Rename(file.Name(), realPath)
	if err != nil {
		log.Fatalf("reduce task: os.Rename() error: %s", err)
	}

	// {
	// 	// debug
	// 	fmt.Println("realPath: ", realPath)
	// }

	ok := call("Coordinator.ReduceFinish", &finishPkt, &finishReply)
	if !ok {
		fmt.Println("Reduce Task failed!")
	}
}

func quitTask() {
	quit = true
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	quit = false

	// Your worker implementation here.
	pid := os.Getpid()
	for !quit {
		reply := RequestReply{}
		ok := call("Coordinator.Request", pid, &reply)
		if !ok {
			fmt.Println("call() get an error!")
			break
		}

		// {
		// 	// debug
		// 	fmt.Println("worker ", pid, " : ", reply.Task)
		// }

		switch reply.Task {
		case PendingTask:
			pendingTask()
			// debug
			// fmt.Println(pid, " done pendingTask()")
		case MapTask:
			mapTask(mapf, &reply)
			// debug
			// fmt.Println(pid, " done mapTask()")
		case ReduceTask:
			reduceTask(reducef, &reply)
			// debug
			// fmt.Println(pid, " done reduceTask()")
		case QuitTask:
			quitTask()
			// debug
			// fmt.Println(pid, " done quitTask()")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
