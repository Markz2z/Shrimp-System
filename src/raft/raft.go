package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "sync"
import "time"
import "math/rand"
import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	STOPPED			= "stopped"
	INITIALIZED 	= "initialized"
	FOLLOWER 		= "follower"
	CANDIDATE 		= "candidate"
	LEADER 			= "leader"
	SNAPSHOTTING 	= "snapshotting"
)


const (
	HEARTBEAT_INTERVAL = time.Millisecond * 50
	MAX_TIMEOUT = time.Millisecond * 500
	MIN_TIMEOUT = time.Millisecond * 200
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             	sync.Mutex
	peers         	[]*labrpc.ClientEnd
	persister    	*Persister
	me             	int // index into peers[]

	//persistent state on all server
	currentTerm 	int
	voteFor      	int
	logs            []interface{}
	logs_term		[]int

	//volatile state on all server
	commitIndex 	int
	lastApplied		int

	//volatile state on leader
	nextIndex   	[]int
	matchIndex 		[]int

	state	   		string
	applyCh			chan ApplyMsg

	granted_votes_count int
	timer			*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return currentTerm, state==LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	enc.Encode(rf.logs_term)
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		dec.Decode(&rf.currentTerm)
		dec.Decode(&rf.votedFor)
		dec.Decode(&rf.logs)
		dec.Decode(&rf.logs_term)
	}
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term 			int
	CandidateId 	int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term 		int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) requestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	may_grant_vote := true
	if len(rf.logs) > 0 {
		//I.current server's log must not newer that the candidate
		//II.if the term of current server is the same as the candidate
		//   the candidate must have more logs than current server
		//Or current server will never vote for this candidate
		if rf.logs_term[len(rf.logs)-1] > args.LastLogTerm ||
			(rf.logs_term[len(rf.logs)-1] == args.LastLogTerm && len(rf.logs) > args.LastLogIndex) {
				may_grant_vote = false
		}
	}
	DEBUG("Got vote request frome %v, may grant vote to %v\n", args, may_grant_vote)

	if args.Term < rf.currentTerm {
		DEBUG("Got vote request vote with term %v is rejceted\n", args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		DEBUG("vote for %v\n", rf.votedFor)
		//one machine could only vote for one machine
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			rf,persist()
		}
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		DEBUG("Got vote request with term %v follow it\n", args.Term)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if may_grant_vote {
			rf.voteFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()

		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		reply.Term = args.Term
		return
	}
}

func (rf *Raft) handleVotesResult(reply *RequestVoteReply) {

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	req_args := RequestVoteArgs{
		Term:	rf.currentTerm,
		CandidateId:	rf.me,
		LastLogIndex:	len(rf.logs) - 1,
	}
	if(req_args.LastLogIndex > 0) {
		req_args.LastLogTerm = rf.logs_term[LastLogIndex]
	}

	for peer := 0; peer < len(rf.peers); ++peer {
		if peer == rf.me {
			continue
		}
		go func(idx int) {
			var reply RequestVoteReply;
			ok := rf.peers[idx].Call("Raft.requestVote", req_args, &reply)
			if ok {
				rf.handleVotesResult(reply)
			}
		}(peer)
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//Heartbeat
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer == time.NewTimer(time.Hour)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	new_timeout := HEARTBEAT_INTERVAL
	if rf.state != LEADER {
		//generate a random value, becasuse every Follower's cycle is different
		val, _ := crand.Int(crand.Reader, big.NewInt(Int64(MAX_TIMEOUT - MIN_TIMEOUT)))
		new_timeout = MIN_TIMEOUT + time.Duration(val.Int64())
	}
	rf.timer.Reset(new_timeout)
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.voteFor = 0
	rf.log = make([]interface{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetTimer()

	return rf
}