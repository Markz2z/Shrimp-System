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
import "time"
import "math/rand"
import "bytes"
import "encoding/gob"
import "fmt"

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}


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
	STOPPED         = "stopped"
	INITIALIZED     = "initialized"
	FOLLOWER        = "follower"
	CANDIDATE       = "candidate"
	LEADER          = "leader"
	SNAPSHOTTING    = "snapshotting"
)

const (
	HeartbeatCycle = time.Millisecond * 50;
	ElectionMinTime = 200;
	ElectionMaxTime = 500; 
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int // index into peers[]

	//persistent state on all server
	currentTerm     int
	votedFor        int
	logs            []interface{}
	logs_term       []int

	//volatile state on all server
	commitIndex 	int
	lastApplied		int

	//volatile state on leader
	nextIndex   	[]int
	matchIndex 		[]int

	state	   		string
	applyCh			chan ApplyMsg

    //count of being voted
	granted_votes_count int
	timer           *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state==LEADER
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
//[Lost Vote]1.Reply false if candidate's term is less than currentTerm
//[Grant Vote]2.if votedFor is null or candidate's id,  and candidate's log is at least as up-to-date as receiver's log, grant vote
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	may_grant_vote := true

	//detect whether should vote to this candidate
	if len(rf.logs) > 0 {
		//I.current server's log must not newer than the candidate
		//II.if the term of current server is the same as the candidate
		//   the candidate must have more logs than current server
		//Or current server will never vote for this candidate
		if rf.logs_term[len(rf.logs) - 1] > args.LastLogTerm ||
		   (rf.logs_term[len(rf.logs)-1] == args.LastLogTerm && 
		   len(rf.logs) - 1 > args.LastLogIndex) {
				may_grant_vote = false
		}
	}
	//debug("Got vote request from %v, may grant vote to %v\n", args, may_grant_vote)

	if args.Term < rf.currentTerm {
		//debug("Got vote request vote with term %v is rejceted\n", args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		//debug("vote for %v\n", rf.votedFor)
		//one machine could only vote for one machine
		if rf.votedFor == -1 && may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		if reply.VoteGranted {
			debug("server[%v] is vote for server[%v]\n", rf.me, args.CandidateId)
		}
		reply.Term = rf.currentTerm
		return
	}
 
	if args.Term > rf.currentTerm {
		//debug("Got vote request with term %v follow it\n", args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if may_grant_vote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()

		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		if reply.VoteGranted {
			debug("server[%v] is vote for server[%v]\n", rf.me, args.CandidateId)
		}
		reply.Term = args.Term
		return
	}
}

func majority(n int) int {
	return n/2+1;
}

func (rf *Raft) handleVotesResult(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		//debug("[ERROR] Reply Term%v is old than Raft%v currentTerm%v\n", reply.Term, rf.me, rf.currentTerm)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.granted_votes_count += 1
		if rf.granted_votes_count >= majority(len(rf.peers)) {
			rf.state = LEADER
			debug("server[%v] is Leader at Term%v!\n", rf.me, rf.currentTerm);
			for i:=0 ;i < len(rf.peers); i+=1 {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			rf.resetTimer()
		}
		return
	}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply){
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			rf.handleVotesResult(reply)
		}
	}()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	index := len(rf.logs)
	rf.logs = append(rf.logs, command)
	rf.logs_term = append(rf.logs_term, rf.currentTerm)
	rf.persist()

	return index + 1, rf.currentTerm, rf.state==LEADER
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	
}

type AppendEntryArgs struct {
	Term        	int
	Leader_id   	int
	prevLogIndex	int
	prevLogTerm		int
	entries 		[]interface{}
	leaderCommit	int
    entries_term    []int
}

type AppendEntryReply struct {
	Term    		int
	Success 		bool
    CommitIndex     int
}

func (rf *Raft) commitLogs() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    for i:=rf.lastApplied + 1;i <= rf.commitIndex;i+=1 {
        rf.applyCh <- ApplyMsg{ Index:i+1, Command: rf.logs[i] }
    }
    rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

    //return false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1

		reply.Term = args.Term
		if args.prevLogIndex >= 0 && len(rf.logs) - 1 < args.prevLogIndex || 
			rf.logs_term[args.prevLogIndex] != args.prevLogTerm {
			//(args.prevLogIndex < len(rf.logs) && rf.logs_term[args.prevLogIndex] != args.prevLogTerm) {
			//debug("server[%v] prevLogIndex:%v  not equal log_length:%v\n", rf.me, args.prevLogIndex, len(rf.logs));
            reply.Success = false
			reply.Term = rf.currentTerm
			rf.resetTimer()
            return
		}
        //If an existing entry conflicts with a new one (Entry with same index but different terms) 
        //delete the existing entry and all that follow it
        rf.logs = rf.logs[:args.prevLogIndex]
        rf.logs_term = rf.logs_term[:args.prevLogIndex]
        rf.logs = append(rf.logs, args.entries...)
        rf.logs_term = append(rf.logs_term, args.entries_term...)

        if args.leaderCommit < len(rf.logs) - 1 {
        	rf.commitIndex = args.leaderCommit
        }else {
        	rf.commitIndex = len(rf.logs) - 1
        }
        if len(rf.logs) >= args.leaderCommit {
            go rf.commitLogs()
        }
        reply.CommitIndex = rf.commitIndex
        reply.Success = true
	}
	rf.persist()
	rf.resetTimer()
}

//
//Handle AppendEntry result
//
func (rf *Raft) handleAppendEntries(reply *AppendEntryReply, idx int, prevLogIdxLast int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != LEADER {
        return
    }

    //Leader should degenerate to Follower
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.state = FOLLOWER
        rf.votedFor = -1
        rf.resetTimer()
        return
    }

    if reply.Success {
        rf.nextIndex[idx] = reply.CommitIndex
        rf.matchIndex[idx] = reply.CommitIndex
        reply_count := 0
        for i:=0;i<len(rf.peers);i+=1 {
            if i==rf.me {
                continue
            }
            if rf.matchIndex[i] > rf.matchIndex[idx] {
                reply_count += 1
            }
        }
        if reply_count >= majority(len(rf.peers)) &&
           rf.commitIndex < rf.matchIndex[idx] &&
           rf.logs_term[rf.matchIndex[idx]]==rf.currentTerm {
            rf.commitIndex = rf.matchIndex[idx]
            go rf.commitLogs()
        }
    }else {
        var args AppendEntryArgs
        if(prevLogIdxLast > 0 ) {
            args = AppendEntryArgs {
                Term:        rf.currentTerm,
                Leader_id:   rf.me,
                prevLogIndex: prevLogIdxLast - 1,
                leaderCommit: rf.commitIndex,
                prevLogTerm:  rf.logs_term[prevLogIdxLast - 1],
            }
            go func() {
                var reply AppendEntryReply
                ok := rf.peers[idx].Call("Raft.AppendEntries", args, &reply)
                if ok {
                    rf.handleAppendEntries(&reply, idx, prevLogIdxLast - 1)
                }
            }()
        }
    }
}


//
//Send AppendEntry Function
//Always call from Leader to Followers
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntryArgs, reply *AppendEntryReply) {
	go func(idx int) {
		ok := rf.peers[idx].Call("Raft.AppendEntries", args, reply)
		if ok {
			rf.handleAppendEntries(reply, idx, args.prevLogIndex)
		}
	}(server)
}

//
//Follower don't get a Heartbeat Response from the Leader
//
func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		//rf.granted_votes_count = 1
		rf.persist()
		args := RequestVoteArgs{
			Term:            rf.currentTerm,
			CandidateId:     rf.me,
		}

		if(len(rf.logs) > 0) {
			args.LastLogIndex = len(rf.logs) - 1;
			args.LastLogTerm = rf.logs_term[args.LastLogIndex]
		}else {
			args.LastLogIndex = 0
			args.LastLogTerm = 0
		}

		rf.granted_votes_count = 1
		for server := 0; server < len(rf.peers);server += 1 {
			if server == rf.me {
				continue
			}
			var reply RequestVoteReply
			rf.sendRequestVote(server, args, &reply)
		}
		rf.granted_votes_count = 1
		debug("server[%v] is send vote request for it self at Term%v\n", rf.me, rf.currentTerm)
	} else {
		for server := 0;server < len(rf.peers); server += 1 {
			if(server==rf.me) {
				continue
			}
			var reply AppendEntryReply
			args := AppendEntryArgs {
			    Term:           rf.currentTerm,
			    Leader_id:      rf.me,
			    prevLogIndex:   rf.nextIndex[server] - 1,
			    leaderCommit:   rf.commitIndex,
		    }
		    if args.prevLogIndex > 0 {
		    	args.prevLogTerm = rf.logs_term[args.prevLogIndex]
		    }else {
		    	args.prevLogTerm = -1
		    }
		    if rf.nextIndex[server] <= len(rf.logs) {
		    	args.entries = rf.logs[rf.nextIndex[server]:]
                args.entries_term = rf.logs_term[rf.nextIndex[server]:]
		    }
		    debug("server[%v] is send heartbeat at Term%v\n", rf.me, rf.currentTerm)
			rf.sendAppendEntries(server, args, &reply)
		}
	}
	rf.resetTimer()
}
//
//Heartbeat
//generate a random value for every server between 150~300
//
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 10000)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	new_timeout := HeartbeatCycle
	if rf.state != LEADER {
		new_timeout = time.Millisecond * time.Duration(ElectionMinTime + rand.Int63n(ElectionMaxTime - ElectionMinTime))
		debug("server[%v]'s random timeout is: %vms\n", rf.me, new_timeout)
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
	rf.votedFor = -1
	rf.logs = make([]interface{}, 0)
	rf.logs_term = make([]int, 0)
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
