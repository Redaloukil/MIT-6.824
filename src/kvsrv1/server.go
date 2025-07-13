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

type KVStateValue struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	state map[string]KVStateValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.state = make(map[string]KVStateValue)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	stateValue, exists := kv.state[args.Key]

	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = stateValue.value
	reply.Version = stateValue.version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.state[args.Key]

	if !exists && args.Version == 0 {
		kv.state[args.Key] = KVStateValue{
			value:   args.Key,
			version: 0,
		}

		reply.Err = rpc.ErrNoKey
		return
	}

	if value.version != args.Version {
		reply.Err = rpc.ErrVersion
	} else {
		kv.state[args.Key] = KVStateValue{
			value:   args.Value,
			version: args.Version,
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
