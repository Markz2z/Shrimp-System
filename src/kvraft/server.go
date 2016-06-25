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

const Debug = 0

const(
	STATUS_FOLLOWER = false
	STATUS_LEADER = true
)

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

	pendingOps map[int][]*P_Op
	data     map[string]string
	persister *raft.Persister
	op_count map[int64]int64
}

func (kv *RaftKV) ExecOp(op Op, op_reply *OpReply) {
	op_idx, _, is_leader := kv.rf.Start(op)
	if !is_leader {
		op_reply.IsLeader = STATUS_FOLLOWER
		DPrintf("This server is not Leader\n")
		return
	}

	waiter := make(chan bool, 1)
	DPrintf("Append to pendingOps op_idx:%v  op:%v\n", op_idx, op)
	kv.mu.Lock()
	kv.pendingOps[op_idx] = append(kv.pendingOps[op_idx], &P_Op{flag: waiter, op: &op})
	kv.mu.Unlock()
	//DPrintf("@@@kv.pendingOps[%v]:%v\n", op_idx, kv.pendingOps[op_idx])
	var ok bool
	timer := time.NewTimer(TIMEOUT)
	select {
	case ok = <-waiter: 
	case <-timer.C:
		DPrintf("Wait operation apply to state machine exceeds timeout....\n")
		ok = false
	}

	if !ok {
		DPrintf("Wrong leader\n")
		op_reply.IsLeader = STATUS_FOLLOWER
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	op_reply.IsLeader = STATUS_LEADER
	DPrintf("This server is Leader")
	if op.Type == OpGet {
		DPrintf("Get Key:%v Value:%v\n", op.Key, op.Value)
		op_reply.Value = kv.data[op.Key]
	}
}

//get a ApplyMsg(a kind of redo log), and then analyze the log and execute it.
func (kv *RaftKV) Apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var args Op
	args = msg.Command.(Op)
	if kv.op_count[args.Client] >= args.Id {
		DPrintf("Duplicate operation\n")
	}else {
		switch args.Type {
		case OpPut:
			DPrintf("Put Key/Value %v/%v\n", args.Key, args.Value)
			kv.data[args.Key] = args.Value
		case OpAppend:
			DPrintf("Append Key/Value %v/%v\n", args.Key, args.Value)
			kv.data[args.Key] = kv.data[args.Key] + args.Value
		default:
		}
		kv.op_count[args.Client] = args.Id
	}

	//DPrintf("@@@Index:%v len:%v content:%v\n", msg.Index, len(kv.pendingOps[msg.Index]), kv.pendingOps[msg.Index])
	//DPrintf("@@@kv.pendingOps[%v]:%v\n", msg.Index, kv.pendingOps[msg.Index])
	for _, i := range kv.pendingOps[msg.Index] {
		if i.op.Client==args.Client && i.op.Id==args.Id {
			DPrintf("Client:%v %v, Id:%v %v", i.op.Client, args.Client, i.op.Id, args.Id)
			i.flag <- true
		}else {
			DPrintf("Client:%v %v, Id:%v %v", i.op.Client, args.Client, i.op.Id, args.Id)
			i.flag <-false
		}
	}
	delete(kv.pendingOps, msg.Index)
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
	gob.Register(OpReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int][]*P_Op)
	kv.op_count = make(map[int64]int64)
	go func () {
		for msg:= range kv.applyCh{
			//msg := <-kv.applyCh
			go kv.Apply(&msg)
		}
	}()

	return kv
}