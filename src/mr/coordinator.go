package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskPhase int

const (
	StartPhase TaskPhase = iota
	MapPhase
	ReducePhase
	QuitPhase
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	curPhase    TaskPhase
	mTaskFiles  []string
	mTaskStates []TaskState
	mWorkerID   []int
	rTaskFiles  [][]string
	rTaskStates []TaskState
	rWorkerID   []int
	mux         sync.Mutex // global lock in c
}

// TODO: commentary
func (c *Coordinator) getAnIdleMapTask() int {
	idx := -1
	// TODO
	return idx
}

// TODO: commentary
func (c *Coordinator) updateMapTaskState(idx int, res *MapFinishArgs) {
	// TODO
}

// the timer goroutine should reset c.mTaskStates[IDX] as Compelted
// if c.mTaskStates[IDX] != Completed.
func (c *Coordinator) resetMapTaskState(pid int, idx int) {
	c.mux.Lock()
	if c.mWorkerID[idx] == pid && c.mTaskStates[idx] != Completed {
		c.mTaskStates[idx] = Idle
		c.mWorkerID[idx] = 0
	}
	c.mux.Unlock()
}

// TODO: commentary
func (c *Coordinator) getAnIdleReduceTask() int {
	idx := -1
	// TODO
	return idx
}

// TODO: commentary
func (c *Coordinator) updateReduceTaskState(idx int, res *ReduceFinishArgs) {
	// TODO
}

// the timer goroutine should reset c.rTaskStates[IDX] as Compelted
// if c.rTaskStates[IDX] != Completed.
func (c *Coordinator) resetReduceTaskState(pid int, idx int) {
	c.mux.Lock()
	if c.rWorkerID[idx] == pid && c.rTaskStates[idx] != Completed {
		c.rTaskStates[idx] = Idle
		c.rWorkerID[idx] = 0
	}
	c.mux.Unlock()
}

// create a 10s timer goroutine and execute reset(idx)
// if timeout event occurs
func resetIfTimeout(pid int, idx int, reset func(int, int)) {
	go func() {
		<-time.After(10 * time.Second)
		reset(pid, idx)
	}()
}

// tryToNextTaskPhase tries to get the whole work transfered to
// next phase if necessary
func (c *Coordinator) tryToNextTaskPhase() {
	// {
	// 	// debug
	// 	fmt.Println("current task phase: ", c.curPhase)
	// }
	switch c.curPhase {
	case StartPhase:
		c.curPhase = MapPhase
	case MapPhase:
		next := true
		c.mux.Lock()
		for _, st := range c.mTaskStates {
			if st != Completed {
				next = false
				break
			}
		}
		// {
		// 	// debug
		// 	for i, st := range c.mTaskStates {
		// 		fmt.Println(i, " : ", st)
		// 	}
		// 	fmt.Println()
		// }

		c.mux.Unlock()
		if next == true {
			c.curPhase = ReducePhase
		}
	case ReducePhase:
		next := true
		c.mux.Lock()
		for _, st := range c.rTaskStates {
			if st != Completed {
				next = false
				break
			}
		}
		c.mux.Unlock()
		if next == true {
			c.curPhase = QuitPhase
		}
	case QuitPhase:
	}
}

// Your code here -- RPC handlers for the worker to call.

/*
	RPC format:
	func (t *T) MethodName(argType T1, replyType *T2) error
*/

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// wokers call this RPC to get tasks and relative args
func (c *Coordinator) Request(pid int, reply *RequestReply) error {
	reply.Task = PendingTask
	switch c.curPhase {
	case StartPhase:
		reply.Task = PendingTask
	case MapPhase:
		idx := -1
		c.mux.Lock()
		for i := range c.mTaskStates {
			// if c.mWorkerID[i] == 0 && c.mTaskStates[i] == Idle {
			if c.mWorkerID[i] == 0 {
				// {
				// 	// debug
				// 	fmt.Println("map task ", i, " to goroutine ", pid)
				// 	fmt.Println("map task state: ", c.mTaskStates[i])
				// }
				idx = i
				c.mTaskStates[i] = InProgress
				c.mWorkerID[i] = pid
				break
			}
		}
		c.mux.Unlock()
		if idx != -1 {
			reply.Task = MapTask
			reply.MapNum = idx
			reply.NReduce = c.nReduce
			reply.InputFilename = c.mTaskFiles[reply.MapNum]
			resetIfTimeout(pid, reply.MapNum, c.resetMapTaskState)
		}
	case ReducePhase:
		idx := -1
		c.mux.Lock()
		for i := range c.rTaskStates {
			// if c.rWorkerID[i] == 0 && c.rTaskStates[i] == Idle {
			if c.rWorkerID[i] == 0 {
				idx = i
				c.rTaskStates[i] = InProgress
				c.rWorkerID[i] = pid
				// {
				// 	// debug
				// 	fmt.Println("reduce task ", i, " to goroutine ", pid)
				// }
				break
			}
		}
		c.mux.Unlock()
		if idx != -1 {
			reply.Task = ReduceTask
			reply.ReduceNum = idx
			reply.TempFilenames = c.rTaskFiles[idx]
			resetIfTimeout(pid, idx, c.resetReduceTaskState)
		}
	case QuitPhase:
		reply.Task = QuitTask
	}
	return nil
}

// workers call this RPC to notify the coordinator that its map task is done
func (c *Coordinator) MapFinish(args *MapFinishArgs, reply *MapFinishReply) error {
	if args.Number >= 0 {
		temps := make([][]string, c.nReduce)
		// {
		// 	// debug
		// 	for _, f := range args.TempFilenames {
		// 		fmt.Println("debug:", f)
		// 	}
		// }
		for _, f := range args.TempFilenames {
			fn := filepath.Base(f)
			parts := strings.Split(fn, "-")
			y, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println(f, "format error!")
				continue
			}
			temps[y] = append(temps[y], f)
		}

		c.mux.Lock()
		if c.mWorkerID[args.Number] == args.Pid {
			c.mTaskStates[args.Number] = Completed
			for i, files := range temps {
				c.rTaskFiles[i] = append(c.rTaskFiles[i], files...)
			}
		}
		c.mux.Unlock()
	} else {
		idx := (args.Number + 1) * -1
		c.mux.Lock()
		c.mWorkerID[idx] = 0
		c.mux.Unlock()
	}
	// debug
	// fmt.Println(args.Pid, " done Map Task in service goroutine!")
	return nil
}

// workers call this RPC to notify the coordinator that its reduce task is done
func (c *Coordinator) ReduceFinish(args *ReduceFinishArgs, reply *ReduceFinishReply) error {
	if args.Number >= 0 {
		c.mux.Lock()
		if args.Pid == c.rWorkerID[args.Number] {
			c.rTaskStates[args.Number] = Completed
		}
		c.mux.Unlock()
	} else {
		idx := (args.Number + 1) * -1
		c.mux.Lock()
		c.rWorkerID[idx] = 0
		c.mux.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for c.curPhase != QuitPhase {
		c.tryToNextTaskPhase()
	}

	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.curPhase = StartPhase
	c.mTaskFiles = files
	{
		// debug
		// fmt.Println(c.mTaskFiles)
	}
	c.mTaskStates = make([]TaskState, len(files))
	c.mWorkerID = make([]int, len(files))
	c.rTaskFiles = make([][]string, nReduce)
	c.rTaskStates = make([]TaskState, nReduce)
	c.rWorkerID = make([]int, nReduce)
	c.mux = sync.Mutex{}

	c.server(sockname)
	return &c
}
