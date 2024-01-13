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

import (
	//	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// log
	logger *Log

	// state
	state       raftState
	leaderExist bool

	// ticker
	electionTimeoutTicker *time.Ticker
	heartBeatTicker       *time.Ticker

	// persistent state
	currentTerm int
	voteFor     int
	log         []*LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state(leader only)

}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate's Id who is requesting vote
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	// for debug
	VoteId int
}

// what changed in RequestVote RPC handler?
// ************************************
// currentTerm, voteFor, electionTimeoutTicker
// ************************************
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	switch rf.state {
	case LEADER:
		// if there is a leader, practical won't case it.
		// logg.log2A.Panicf("[peer %d | %s | term %d]: %s", rf.me, rf.state, rf.currentTerm,  "Leader get RequestVote RPC, something get wrong.")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteId = rf.me
	case CANDIDATE:
		// if self, vote for self.
		if args.CandidateId == rf.me {
			rf.voteFor = rf.me

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			reply.VoteId = rf.me
			logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("%d is candidate, vote for %d", rf.me, reply.VoteId))
		} 
		if args.CandidateId != rf.me {
			// from another candidate, reject it, since we should vote for self.
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			reply.VoteId = rf.me
			logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("%d is candidate, reject %d", rf.me, reply.VoteId))
		}
	case FOLLOWER:
		// first update self's term if term < candidate's.
		if args.Term > rf.currentTerm {

			rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
			rf.currentTerm = args.Term

		}
		// if candidate satisfy rule of being a leader, vote for the canditate.
		// reject
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			reply.VoteId = rf.me
		}
		// accept
		if rf.voteFor == NULL || rf.voteFor == args.CandidateId {

			rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
			rf.voteFor = args.CandidateId

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			reply.VoteId = rf.me
			logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("Accept request vote, %d vote for %d", rf.me, args.CandidateId))
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			reply.VoteId = rf.me
			logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("Reject request vote, %d reject for %d", rf.me, args.CandidateId))
		}
		// TODO: candidate's log is at least as up-to-date as receiver's log
		

	}
	rf.mu.Unlock()

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int //leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat
	if len(args.Entries) == 0 {
		rf.mu.Lock()
		switch rf.state {
		case LEADER:
			// pass
			
			rf.voteFor = NULL

			reply.Term = rf.currentTerm
			reply.Success = true
		case CANDIDATE:
			// leader's current term >= candidate's current term 
			if args.Term >= rf.currentTerm {
				// turn into follower

				rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
				rf.currentTerm = args.Term
				rf.state = FOLLOWER
				// update voteFor
				rf.voteFor = NULL
				
				reply.Term = rf.currentTerm
				reply.Success = true
				logg.printf2A(rf.me, rf.state, rf.currentTerm, "Candidate received a hearbeat, turn into follower")
			} else {
				// reject the heart beat, continue candidate
			
				reply.Term = rf.currentTerm
				reply.Success = false
			
				logg.printf2A(rf.me, rf.state, rf.currentTerm, "The request's term < me's term, reject the request.")
			}
		case FOLLOWER:
			if rf.currentTerm < args.Term {
				
				rf.currentTerm = args.Term
			
			}
			// update voteFor
		
			rf.voteFor = NULL
			rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
		
			reply.Term = rf.currentTerm
			reply.Success = true
		}
	}
	rf.mu.Unlock()
	// append entries phase

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// what changed in ET Ticker goroutine?
// ************************************
// currentTerm, state, 
// ************************************
// 
// send heartbeat
func (rf *Raft) HBTicker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		// This is used to heartbeat.

		<-rf.heartBeatTicker.C
		rf.heartBeatTicker.Reset(randomTimeForHeartBeat())
		switch rf.state {
		case LEADER:
			args := newHeartBeatArgs(rf.currentTerm, rf.me)
			reply := newHeartBeatReply()
			logg.printf2A(rf.me, rf.state, rf.currentTerm, "Leader trying to send heart beat.")
			for index := range rf.peers {
				rf.sendAppendEntries(index, args, reply)
				// if leader's term < other, turn into follower.
				if rf.currentTerm < reply.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.mu.Unlock()
					logg.printf2A(rf.me, rf.state, rf.currentTerm, "Leader's term < other, turn into follower.")
				}
			}
		case CANDIDATE:
			continue
		case FOLLOWER:
			continue
		default:
			log.Fatalln("HBTicker: error raft state.")
		}
		

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// what changed in ET Ticker goroutine?
// ************************************
// currentTerm, state, electionTimeoutTicker, 
// ************************************
// used to ticker election timeout
func (rf *Raft) ETTicker() {
	for !rf.killed() {
		<-rf.electionTimeoutTicker.C
		switch rf.state {
		case LEADER:
			continue
		case CANDIDATE:
			// restart election
			rf.mu.Lock()
			rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.mu.Unlock()
			// requestVote RPC
			logg.printf2A(rf.me, rf.state, rf.currentTerm, "Candidate be not able to become leader, restart election.")
			args := newRequestVoteArgs(rf.currentTerm, rf.me, NULL, NULL)
			reply := newRequestVoteReply()
			totalGranted := 0
			for index := range rf.peers {
				rf.sendRequestVote(index, args, reply)
				// when vote for leader, if candicate's term < other's, turn into follower.
				if rf.currentTerm < reply.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.mu.Unlock()
					break
				}
				if reply.VoteGranted {
					totalGranted++
					logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("%d receive vote from %d", rf.me, reply.VoteId))
				}
			}
			// get the major granted, success to become leader
			if totalGranted > (len(rf.peers) / 2) {
				rf.state = LEADER
				logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("In restart election phase, candidate get most of grant(%d), become a leader.", totalGranted))
				// send a heart beat
				hbArgs := newHeartBeatArgs(rf.currentTerm, rf.me)
				hbReply := newHeartBeatReply()
				for index := range rf.peers {
					rf.sendAppendEntries(index, hbArgs, hbReply)
				}
				rf.electionTimeoutTicker.Reset(randomTimeForHeartBeat())
				logg.printf2A(rf.me, rf.state, rf.currentTerm, "Leader(candidate before) start a heartbeat.")
			}

		case FOLLOWER:
			// start election
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.electionTimeoutTicker.Reset(randomTimeForElectionTimeout())
			rf.mu.Unlock()
			// requestVote RPC
			logg.printf2A(rf.me, rf.state, rf.currentTerm, "Follower turn into candidate, issue a RequestVote RPC.")
			args := newRequestVoteArgs(rf.currentTerm, rf.me, NULL, NULL)
			reply := newRequestVoteReply()
			totalGranted := 0
			for index := range rf.peers {
				rf.sendRequestVote(index, args, reply)
				// when vote for leader, if candicate's term < other's, turn into follower.
				if rf.currentTerm < reply.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.mu.Unlock()
					break
				}
				if reply.VoteGranted {
					logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("%d receive vote from %d", rf.me, reply.VoteId))
					totalGranted++
				}
			}
			// get the major granted, success to become leader
			if totalGranted > (len(rf.peers) / 2) {
				rf.state = LEADER
				logg.printf2A(rf.me, rf.state, rf.currentTerm, fmt.Sprintf("Follower turn into candidate, get most of grant(%d), become a leader.", totalGranted))
				// send a heart beat
				hbArgs := newHeartBeatArgs(rf.currentTerm, rf.me)
				hbReply := newHeartBeatReply()
				for index := range rf.peers {
					rf.sendAppendEntries(index, hbArgs, hbReply)
				}
				rf.electionTimeoutTicker.Reset(randomTimeForHeartBeat())
				logg.printf2A(rf.me, rf.state, rf.currentTerm, "Leader(follower before) start a heartbeat.")
			}
		}
	}
}



// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.electionTimeoutTicker = time.NewTicker(randomTimeForElectionTimeout())
	rf.heartBeatTicker = time.NewTicker(randomTimeForHeartBeat())
	rf.leaderExist = true

	rf.currentTerm = 0
	rf.voteFor = NULL
	rf.log = make([]*LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ETTicker()
	go rf.HBTicker()

	return rf
}
