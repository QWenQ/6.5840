# Key/Value Server Lab Design

## Workflow

## Q&As

Q: thread model?

1. server, client and service goroutines for RPCs;

Q: at-most-once semantics for `Put(key, value, version)`?

for every `key`, `Put()` can only set specific `value` for each `version`

## Design

### Feature1.0

Key/value server with reliable network:
    1. `Put(key, value, version)` installs or replaces the value for a particular key in the map;
    2. `Get(key)` fetches the current value for the key and its
    associated version;

#### Data Structures and Functions

```Go
in server.go:

type ValueVersion struct {
    value string
    version rpc.Tversion
}

type KVServer struct {
    mu sync.Mutex
    // new field: key/{value, version} pairs container
    kvs map[string]ValueVersion
}

```

#### Algorithms

```Go
in server.go:

/*
    do as comment says
*/
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply);

/*
    do as comment says
*/
func (kv *KVServer) Put(args *rpc.GetArgs, reply *rpc.GetReply);


in client.go:

/*
    do as comment says
*/
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err); 

/*
    do as comment says
*/
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err;

```

#### Synchronization

1. shared resource:
    1. `KVServer.kvs`;
2. racing scenarios:
    1. `Put(key, val1, v1)` and `Put(key, val2, v1)` occur
    concurrently;
    2. `Put(key, val1, v1)` and `Get(key)` occur concurrently;
3. solution:
    1. `KVServer.mu.Lock()` before accessing `KVServer.kvs`;
    1. `KVServer.mu.Unlock()` after accessing `KVServer.kvs`;

### Feature2.0

Implementing a lock using key/value clerk:
    1. mutex lock in distributed applications;

#### Data Structures and Functions

```Go

in lock.go:

type Lock struct {
    lockname string
}

```

#### Algorithms

```Go

in lock.go:

/*
    MakeLock registers a new lock in server by calling ck.Put(lockname, "0", 0);
    value 0 indicates the lock is available and vlaue 1 indicates
    the lock is unavailable;
*/
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock

/*
    Acquire keeps trying to call lk.ck.Get(lk.lockname) until the
    client gets value 0 from the server; 
    then the client will try to get lock by calling 
    lk.ck.Put(lk.lockname, "1", version): if Put() fails, the client
    will continue to do Get() loop until the lock is free
*/
func (lk *Lock) Acquire()

/*
    Release frees lock resource: first, the client gets lock version
    by calling lk.ck.Get(lk.lockname); then the client releases the
    lock by calling lk.ck.Put(lk.lockname, "0", version)
*/
func (lk *Lock) Release()

```

#### Synchronization

1. shared resource:
2. racing scenarios:
3. solution:

### Feature3.0

Key/value server with dropped messages

#### Data Structures and Functions

```Go

in client.go:


```

#### Algorithms

```Go

in client.go:

/*
*/
func (ck *Clerk) Get(key str/ng) (string, rpc.Tversion, rpc.Err)

/*
*/
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err
```

#### Synchronization

1. shared resource:
2. racing scenarios:
3. solution:

### Feature4.0

Implementing a lock using key/value clerk and unreliable network

#### Data Structures and Functions

#### Algorithms

#### Synchronization

1. shared resource:
2. racing scenarios:
3. solution:

### FeatureX.0

#### Data Structures and Functions

#### Algorithms

#### Synchronization

1. shared resource:
2. racing scenarios:
3. solution:
