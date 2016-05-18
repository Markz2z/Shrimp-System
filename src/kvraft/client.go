package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd

	leader_id int
	client_id  int64
	cur_op_count	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.client_id = nrand()
	ck.leader_id = 0
	ck.cur_op_count = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var args Op
	args.Key = key
	args.Type = OpGet
	args.Id = atomic.AddInt64(&ck.cur_op_count, 1)
	for {
		var reply OpReply
		ok := ck.servers[ck.leader_id].Call("RaftKV.ExecOp", args, &reply)
		if ok {
			return reply.Value
		}else {
			ck.leader_id = ck.leader_id + 1
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op int) {
	var args Op
	args.Type = op
	args.Key = key
	args.Value = value
	args.Id = atomic.AddInt64(&ck.cur_op_count, 1)
	for {
		var reply OpReply
		ok := ck.servers[ck.leader_id].Call("RaftKV.ExecOp", args, &reply)
		if ok && reply.IsLeader == STATUS_LEADER{
			DPrintf("Call success\n")
			break
		}else {
			ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
