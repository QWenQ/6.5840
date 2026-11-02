# MapReduce Lab Design

## Workflow

Task logic -> Features -> Data Structures and Functions
-> Algorithms -> Synchronizations

## Q&As

Q: When subdividing the map phase into M pieces and the reduce
    phase into R pieces, how to determine M and R?

1. M and R should be much larger than the number of worker machines,
    which is W in this doc. In this MapReduce, R is 10,
    M is determined by the number of input files passed into
    `mrcoordiante.go` and W is defined by the user who can start
    a new worker with command `go run mrworker.go wc.so sock123`.

Q: intermediate files naming convention?

1. mr-X-Y, where X is the Map task number(0 <= X < M), and Y is the
    reduce task number(0 <= Y < R).

Q: state of the task?

1. idle / in-progress / completed  

Q: Map phase logic?

1. each worker sends a Request RPC to the coordiantor to ask for
    a job and the coordinator creates a new service goroutine
    to handle worker's request;
2. the service goroutine allocates an idle state task or overdue
    task to the worker from input file splits, the task is numbered
    as X. After that, the service goroutine updates task state as
    in-progress and set a 10s timer for the task;
3. the worker does the map task X and writes results into different
    mr-X-Y files where Y is computed via hash function in worker;
    After done map task, the worker sends a Finish RPC back to the
    coordinator with all mr-X-Y files info;
4. the coordinator creates a new service goroutine to handle the
    worker's map task results; the service goroutine gets worker's
    RPC message, updates task state as completed and puts
    intermediate file locations into an array according to file's
    suffix Y value.

Q: how does the map phase get through to the reduce phase?

1. there is a state set which indicates whether all splits are
    handled by the workers in map phase. In coordinator, if all
    splits have been transferred to intermediate files, the map
    phase will get through to reduce phase.

Q: Reduce phase logic?

1. each worker sends a Request RPC to the coordiantor to ask for
    a job and the coordinator creates a new service goroutine
    to handle worker's request;
2. in reduce phase, the service goroutine allocates an idle state
    task or overdue task in intermediate file array to the worker;
    the task is tagged as task Y as Y is files' index in the
    intermediate file array; intermediate files suffixed with Y  
    and Y value will be replied back to the worker. After that, the
    service goroutine updates task Y state as Doing and set a 10s
    timer for task Y;
3. the worker does the reduce task and writes results into mr-out-Y
    file; After reduce task is done, the worker sends a Finish RPC  
    back to the coordinator with Y tag info;
4. when getting a Finish RPC, the coordinator updates task Y state
    as Done.

Q: how to finish the entire job?

1. when all reduce task are done, the coordinator quits;
2. when the worker disconnects from the coordinator, it quits;

Q: when there is no more task to allocate to workers between map
    phase and reduce phase, how does worker do?

1. the worker will get a sleep order in an RPC reply, and when it
    wakes up, it continues to ask for a job; once wokers disconnects
    from the coordinator, they should quit;

Q: In scenarios involving tasks that may time out, how can one
    handle cases where multiple tasks attempt to write to the same
    file simultaneously?

1. To ensure that nobody observes partially written files in the
    presence of crashes, the MapReduce paper mentions the trick of
    using a temporary file and atomically renaming it once it is
    completely written. You can use `os.CreateTemp` to create a
    temporary file and `os.Rename` to atomically rename it.
    PS: `os.Getwd()` to get current working directory

Q: what's the thread model of MapReduce?  
    {hint: Package RPC.HandleHttp() method}

1. the coordinator maintains workflow and task state initializaiton.
    It's also a conduit through which the location of intermediate
    file regions is propagated from map tasks to reduce tasks;
2. the service goroutines, one for each RPC, are for task scheduling,
    data handling and task state management;
3. the workers are task executors for pending, map, reduce and quit.
4. a timer goroutine will reset its relative split stask state
    if timeout event occurs;

Q: for a worker recovering from crush state or overdue, what does
    service goroutine do when the worker completes its mapper task?

1. if the map task is in-progress state, the service goroutine
    should accept the worker's results; if the map task is already
    done, the service goroutine should ignore the results but
    reply to the worker with ok message;

Q: which Data Structures and Functions should be chosen?
    map? array? slice?

1. array and slice are good enough.

Q: timer usage?

1. `go` a new goroutine which will reset task state when timout
    event occurs.

Q: Go runs the handler for each RPC in its own thread, so the
    coordinator, as an RPC server, will be concurrent; which shared
    data should be locked to avoid race condition and deadlock?

1. see Design part

Q: for map and reduce task, what args they are expected to get from
    the service goroutine?
    {hint:
        `func Map(filename string, contents string) []mr.KeyValue`
        `func Reduce(key string, values []string) string`
        and workflow in `mrsequential.go`
    }

1. map work: the service goroutine passes `nReduce` and a input
    file location to the map worker; after the woker finishes its
    map task, it returns intermediate files array back to the
    coordiantor;
2. reduce work: the SG passes intermediate files and map task number
    who did the intermediate files to the reduce worker; after the
    worker done its reduce task, it returns a signal back to the
    coordinator to indicate whether the reduce taks successes or
    not.

## Design

### Feature1.0

1. the coordinator will determine the current task phase based on
    the completion status of the tasks.

#### Data Structures and Functions

```Go
in coordinator.go:

type TaskPhase int

const (
    StartPhase TaskPhase = iota
    MapPhase
    ReducePhase
    QuitPhase
)

type Coordinator struct {
    curPhase TaskPhase
}

func (c *Coordinator) tryToNextTaskPhase();

```

#### Algorithms

```Go
in coordinator.go:

/*
    in start phase, the coorinator will transfer start phase to map phase after all variables are initialized;
    in map phase, the coordinator will iterate all split states to
    check if map pahse should be transferred to reduce phase;
    in reduce phase, the coordinator will check all intermediate files
    to check if reduce phase should be transferred to quit phase
*/
func (c *Coordinator) tryToNextTaskPhase();

```

#### Synchronization

1. shared resources: `c.curPhase`;
2. concurrent scenarios:
    1. the coordinatordoes read/write `c.curPhase` to update task
        phase in `c.Done()`;
    2. the service goroutines will read `c.curPhase` to help in
        making task allocation and result handling;
3. solution: `c.curPhase` is just written by the coordinator;
    though service goroutines will read `c.curPhase`, their work
    still depend on those variables accessed with lock and
    `c.curPhase` won't influence the result of these works;
    so `c.curPhase` is lock-free.

### Feature2.0

the coordinator supports 3 types RPC:

1. Request RPC: the worker call this RPC to ask for an idle task;
2. MapFinish RPC: the worker call this to reply map task result;
3. ReduceFinish RPC: the worker call this to reply reduce task
    result;

#### Data Structures and Functions

```Go
in rpg.go:

type TaskType int

const (
    PendingTask TaskType = iota
    MapTask
    ReduceTask
    QuitTask
)

type RequestReply struct {
    Task        TaskType
    // map task fields
    NReduce     int
    MapNum      int
    InputFilename   string
    // reduce task fields
    TempFilenames   []string
}

type MapFinishArgs struct {
    Number int      // map task number
    TempFilenames []string  // intermediate files location with nReduce size
}

type ReduceFinishArgs struct {
    Number int  // reduce task number
}

in coordinator.go:

type Coordinator strut {
    nReduce int
}

// rpc format: func (t *T) MethodName(argType T1, replyType *T2) error

func (c *Coordinator) Request(args *any, reply *RequestReply) error

func (c *Coordinator) MapFinish(args *MapFinishArgs, reply *any) error

func (c *Coordinator) ReduceFinish(args *ReduceFinishArgs, reply *any) error

```

#### Algorithms

```Go
in coordinator.go:

/*
    in map phase, the service goroutine allocates an idle map
    task and start a 10-second timer goroutine;
    in reduce phase, the service goroutine allocates an idle
    reduce task and start a 10-second timer goroutine; 
    in transition phase between map and reduce, the service
    goroutine will ask the worker to wait if there is no idle task;
    in quit phase, the service goroutine wiil order the wokre
    finishing its work;
*/
func (c *Coordinator) Request(args *any, reply *RequestReply) error

/*
    the service goroutine will try to record map task results for
    reduce task if the map task works successfully and the task is
    not done by other workers.
*/
func (c *Coordinator) MapFinish(args *MapFinishArgs, reply *any) error

/*
    the service goroutine will try to record reduce task results 
    if the reduce task works successfully and the task is not done
    by other workers.
*/
func (c *Coordinator) ReduceFinish(args *ReduceFinishArgs, reply *any) error

```

#### Synchronization

see Feature3.0 and Feature4.0

### Feature3.0

1. the coordinator will divide input files into M splits
    for map task;
2. the coordinator will update map/reduce task phase with regard
    to splits info;
3. in map task phase, a Request RPC service goroutine selects
    an idle task from all splits; a Finish RPC service goroutine
    should update relative split task state;
4. a timer goroutine will reset its relative split stask state
    if timeout event occurs;

#### Data Structures and Functions

```Go
in coordinator.go:

type TaskState int

const (
    Idle TaskState = iota
    InProgress
    Completed
)

type Coordinator struct {
    mTaskFiles []string     // M size
    mTaskStates []TaskState // M size
    mux sync.Mutex          // global mutext in Coordinator
}


func (c *Coordinator) getAnIdleMapTask() int;

func (c *Coordinator) updateMapTaskState(idx int, res *MapFinishArgs);

func (c *Coordinator) resetMapTaskState(idx int);

```

#### Algorithms

```Go
in coordinator.go:

/*
    the service goroutine iterates c.mTaskStates[] to gets an idle split
    in position idx if there is at least one idle split;
    c.mTaskStates[idx] will be set as InProgress and idx will be returned;
    if there is no idle split, the func return -1.
*/
func (c *Coordinator) getAnIdleMapTask() int;

/*
    as long as c.mTaskStates[IDX] != Completed, the service goroutine
    should record results in RES and set c.mTaskStates[IDX] as Completed if RES is done successfully.
*/
func (c *Coordinator) updateMapTaskState(idx int, res *MapFinishArgs);

/*
    the timer goroutine should reset c.mTaskStates[IDX] as Compelted
    if c.mTaskStates[IDX] != Completed.
*/
func (c *Coordinator) resetMapTaskState(idx int);
```

#### Synchronization

1. `c.mTaskFiles[]` and `c.mTaskStates` are shared resources by goroutines;
2. concurrent scenarios:
    1. the coordinator in `c.Done()` will check `c.mTaskStates[]` to
    determine should the map task phase get into reduce task phase;
    2. the service goroutines answering Request RPC will iterate
    `c.mTaskStates[]` to get an idle split, update the idle split's
    state as `InProgress` and get relative info from `c.mTaskFiles[]`;
    3. the service goroutines answering MapFinish RPC will update relative
    split's state in `c.mTaskStates[]` as `Completed` if it is not;
    4. the timer goroutine will reset split state as `Idle` in
    `c.mTaskStates[]` if timeout event occurs;
3. solution:
    1. `c.spitFiles[]` is a read-only variable, so it can be
    lock-free; `c.mTaskStates[]` is read/written by the coordinator,
    the service goroutines and the timer goroutines, so its access
    should be locked by `c.mux`;

### Feature4.0

1. the coordinator divides intermediate files into R buckets for
    reduce task;
2. the coordinator will update map/reduce task phase with regard
    to R buckets info;
3. in reduce task phase, a Request RPC service goroutine selects
    an idle task from all buckets; a Finish RPC service goroutine
    should update relative bucket task state;
4. a timer goroutine will reset its relative bucket stask state
    if timeout event occurs;

#### Data Structures and Functions

```Go
in coordinator.go:

type Coordinator struct {
    rTaskFiles [][]string       // intermediate files: M x R size
    rTaskStates []TaskState     // R size
    mux sync.Mutex              // defined in Feature3.0
}

func (c *Coordinator) getAnIdleReduceTask() int;

func (c *Coordinator) updateReduceTaskState(idx int, res *ReduceFinishArgs);

func (c *Coordinator) resetReduceTaskState(idx int);

```

#### Algorithms

```Go
in coordinator.go:

/*
    the service goroutine iterates c.rTaskStates[] to gets an
    idle reduce task in position idx if there exist idle tasks;
    c.rTaskStates[idx] will be set as InProgress and idx will 
    be returned; if there is no idle task, -1 wil be returned.
*/
func (c *Coordinator) getAnIdleReduceTask() int;


/*
    as long as c.rTaskStates[IDX] != Completed, the service 
    goroutine should set c.rTaskStates[IDX] as Completed if 
    reduce task is done successfully.
*/
func (c *Coordinator) updateReduceTaskState(idx int, res *ReduceFinishArgs);

/*
    the timer goroutine should reset c.rTaskStates[IDX] as 
    Compelted if c.rTaskStates[IDX] != Completed.
*/
func (c *Coordinator) resetReduceTaskState(idx int);
```

#### Synchronization

1. shared resources: `c.rTaskStates[]` and `c.rTaskFiles[][]`;
2. concurrent scenarios:
    1. the coordinator, in `c.Done()`, will check `c.rTaskStates[]`
    to determine should the reduce task phase be transferred to quit
    phase;
    2. the service goroutine answering Request RPC will iterate
    `c.rTaskStates[]` to get an `Idle` reduce task, set the task's
    state in `c.rTaskStates[]` as `InProgress` and read
    intermediate file info from `c.rTaskFiles[][]`;
    3. the service goroutine answering ReduceFinish RPC will update
    relative reduce task state in `c.rTaskStates[]` as `Completed`;
    4. the timer goroutine will reset reduce task state as `Idle` in
    `c.rTaskStates[]` if timeout event occurs;
3. solution:
    1. `c.rTaskFiles[][]` is a read-only variable, so it can be
    lock-free; `c.rTaskStates[]` is read/written by the
    coordinator, the service goroutines and the timer goroutines,
    so its access should be locked by `c.mux`;

### Feature5.0

1. the coordinator should notice if a worker hasn't completed its
    task in a reasonable amount of time (for this lab, use ten
    seconds), and give the same task to a different worker.
    So, a timer goroutine should reset its map/reduce task state if
    timeout event, 10s, occurs.

#### Data Structures and Functions

```Go
in coordinator.go:

// TODO: a failing worker's timeout goroutine may destroy
// other succuess worker's work
type Coordinator struct {
    mapWokerID []int
    reduceWorkerID []int
}

func resetIfTimeout(idx int, reset func(int))
```

#### Algorithms

```Go
/*
    create a 10s timer goroutine: if timeout occurs, RESET() will
    reset state as Idle of task IDX;
    RESET func: c.resetMapTaskState and c.resetReduceTaskState
*/
func resetIfTimeout(idx int, reset func(int))
```

#### Synchronization

### Feature6.0

1. task types the service goroutines allocate to the workers are
    4 types: pending, map, reduce and quit;

#### Data Structures and Functions

```Go
in rpc.go:

type TaskType int

const (
    PendingTask TaskType = iota
    MapTask
    ReduceTask
    QuitTask
)

type RequestReply struct {
    Number int           // map task number
    Task TaskType
    FirstArg string 
    SecondArg any // string for map task, []string for reduce task
}


in worker.go:

// QUIT indicates whether the worker should finish its work
val quit bool;

// pending task
func pendingTask();

// map task
func mapTask(mapf func(string, string) []KeyValue, reply *RequestReply);

// reduce task
func reduceTask(reducef func(string, []string) string, reply *RequestReply);

// quit task
func quitTask();

```

#### Algorithms

```Go
in worker.go:
/*
    this func will make the woker sleep 1 second
*/
func pendingTask();

/*
    the func will execute MAPF func with parameters in RequestReply,
    after MAPF is done, it sends the result back to the coordinator.
*/
func mapTask(mapf func(string, string) []KeyValue, reply *RequestReply);

/*
    the func will execute REDUCEF func with parameters in 
    RequestReply, after MAPF is done, it sends the result back to
    the coordinator.
*/
func reduceTask(reducef func(string, []string) string, reply *RequestReply);

/*
    the func set QUIT variable as true, which will finish the
    whole work.
*/
func quitTask();

/*
    the func works in a for loop:
    first it sends a task request to the coordinator by call() func;
    then it waits until a response comes back from the cooridnator;
    then the worker will do as task type info in response; 
    when the task is done, the worker asks a new job again until
    it gets a quit task or disconnects from the coordinator.
*/
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string)
```

#### Synchronization

None

### FeatureX.0

TODO

#### Data Structures and Functions

#### Algorithms

#### Synchronization
