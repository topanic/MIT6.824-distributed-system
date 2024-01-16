package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// three state in raft
type raftState int

const (
	LEADER    raftState = iota
	CANDIDATE 
	FOLLOWER
	NULL int = -1
)


// about log
type LogEntry struct {
	// first is 1.
	Command string
	Term    int // when entry was received by leader
}


// RPC 

func newRequestVoteArgs(term int, candidateId int, lastLogIndex int, lastLogTerm int) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term: term,
		CandidateId: candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
}

func newRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{
		VoteGranted: false,
	}
}

func newHeartBeatArgs(term int, leaderId int) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term: term,
		LeaderId: leaderId,
		Entries: make([]*LogEntry, 0),
	}
}

func newHeartBeatReply() *AppendEntriesReply {
	return &AppendEntriesReply{}
}


// about time
const (
	TickInterval int64 = 30
	// Lower bound of heartbeat timeout. Election is raised when timeout as a follower.
	BaseHeartbeatTimeout int64 = 300
	// Lower bound of election timeout. Another election is raised when timeout as a candidate.	
	BaseElectionTimeout int64 = 1000	
	// Factor to control upper bound of heartbeat timeouts and election timeouts.
	RandomFactor float64 = 0.8	

)

func randomHeartbeatTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63() % BaseHeartbeatTimeout) * RandomFactor)
	return time.Duration(extraTime + BaseHeartbeatTimeout) * time.Millisecond
}

func randomElectionTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63() % BaseElectionTimeout) * RandomFactor)
	return time.Duration(extraTime + BaseElectionTimeout) * time.Millisecond
}
