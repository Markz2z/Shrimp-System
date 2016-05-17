package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const TIMEOUT = time.Second * 3

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type P_Op struct {
	flag    chan bool
	op      *Op
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	pendingOps map[int64][]*P_Op
	data     map[string]string
}

func (kv *RaftKV) ExecOp(op *Op, op_reply *OpReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op_idx, cur_term, is_leader = kv.rf.Start(op)
	if !is_leader {
		op_reply = STATUS_FOLLOWER
		DPrintf("This server is not Leader\n")
		return
	}

	waiter := make(chan bool, 1)
	kv.pendingOps[op_idx] = append(kv.pendingOps[op_idx], &P_Op{flag: waiter, op: &op})

	timer := time.NewTimer(TIMEOUT)
	select {
	case ok := <-waiter: 
	case <-timer.C:
		reply.Status = STATUS_FOLLOWER
		DPrintf("Exceeds Timeout!\n")
		ok = false
		return
	default
	}

	reply.Status = STATUS_LEADER
	if op.OpType == OpGet {
		reply.Value = kv.data[op.Key]
	}
}

func (kv *RaftKV) Apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	args := msg.Command
	switch args.Op {
	case OpPut:
		DPrintf("Put Key/Value %v/%v\n", args.Key, args.Value)
		kv.data[args.Key] = args.Value
	case OpAppend:
		DPrintf("Append Key/Value %v/%v\n", args.Key, args.Value)
		kv.data[args.Key] = kv.data[args.Key] + args.Value
	default:
	}

	for _, x in range 
	delete(kv.pendingOps, msg.Index)
}

func (kv *RaftKV)Appky(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()


}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)

	go func () {
		for {
			msg := <-kv.applyCh
			kv.Apply(&msg)
		}
	}()

	return kv
}
