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

Implementing a lock using key/value clerk

#### Data Structures and Functions

#### Algorithms

#### Synchronization

1. shared resource:
2. racing scenarios:
3. solution:

### Feature3.0

Key/value server with dropped messages

#### Data Structures and Functions

#### Algorithms

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
