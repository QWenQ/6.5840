package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	PendingTask TaskType = iota
	MapTask
	ReduceTask
	QuitTask
)

type RequestReply struct {
	Task TaskType // task the worker should run
	// map task filds
	NReduce       int
	MapNum        int
	InputFilename string
	// reduce task filds
	ReduceNum     int
	TempFilenames []string
}

type MapFinishArgs struct {
	Pid           int      // worker pid
	Number        int      // map task number
	TempFilenames []string // intermediate files locations with nReduce size
}

type MapFinishReply struct{}

type ReduceFinishArgs struct {
	Pid    int // worker pid
	Number int // reduce task number
}

type ReduceFinishReply struct{}
