package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs map[string]ValueVersion // {key, {value, version}} pairs container

}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvs = make(map[string]ValueVersion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	reply.Err = rpc.ErrNoKey
	kv.mu.Lock()
	vv, ok := kv.kvs[args.Key]
	kv.mu.Unlock()
	if ok {
		reply.Value = vv.value
		reply.Version = vv.version
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	reply.Err = rpc.ErrNoKey
	kv.mu.Lock()
	vv, ok := kv.kvs[args.Key]
	if ok {
		if vv.version != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.kvs[args.Key] = ValueVersion{value: args.Value, version: args.Version + 1}
			reply.Err = rpc.OK
		}
	} else if args.Version == 0 {
		kv.kvs[args.Key] = ValueVersion{value: args.Value, version: args.Version + 1}
		reply.Err = rpc.OK
	}
	kv.mu.Unlock()
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
