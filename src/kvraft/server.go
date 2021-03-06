package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
)


// Op structure
type Op struct {
	Type      string
	Key       string
	Value     string
	ClientID  int64
	RequestID int
}

// RaftKV structure that holds Raft instance
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	kvDB   map[string]string
	dup    map[int64]int
	result map[int]chan Op
	killCh chan bool
}

// AppendEntry appends an entry Op and returns a boolean
func (kv *RaftKV) AppendEntry(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]

	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		return op.ClientID == entry.ClientID && op.RequestID == entry.RequestID
	case <-time.After(1000 * time.Millisecond):
		return false
	}
	return false
}

// Run RaftKV
func (kv *RaftKV) run() {
	for {
		select {
		case msg := <-kv.applyCh:
				index := msg.Index
				op := msg.Command.(Op)
				kv.mu.Lock()
				if !kv.isDup(&op) {
					switch op.Type {
					case "Put":
						kv.kvDB[op.Key] = op.Value
					case "Append":
						kv.kvDB[op.Key] += op.Value
					}
					kv.dup[op.ClientID] = op.RequestID
				}
				ch, ok := kv.result[index]
				if ok {
					select {
					case <-kv.result[index]:
					case <-kv.killCh:
						return
					default:
					}
					ch <- op
				} else {
					kv.result[index] = make(chan Op, 1)
				}
				
				kv.mu.Unlock()
		case <-kv.killCh:
			return
		}
	}
}

// Get RPC
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{Type: "Get", Key: args.Key, ClientID: args.ClientID, RequestID: args.RequestID}

	ok := kv.AppendEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK

		kv.mu.Lock()
		reply.Value, ok = kv.kvDB[args.Key]
		if !ok {
			reply.Value = ""
		}
		kv.dup[args.ClientID] = args.RequestID
		kv.mu.Unlock()
	}
}

// PutAppend RPC
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{args.Op,args.Key,args.Value,args.ClientID,args.RequestID}

	ok := kv.AppendEntry(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

// Kill is called by the tester when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill, but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.killCh)
}

// StartKVServer return a RaftKV
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvDB = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.dup = make(map[int64]int)
	kv.killCh = make(chan bool)

	go kv.run()

	return kv
}



// Check duplication
func (kv *RaftKV) isDup(op *Op) bool {
	v, ok := kv.dup[op.ClientID]
	if ok {
		return v >= op.RequestID
	}

	return false
}