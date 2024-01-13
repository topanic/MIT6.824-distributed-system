package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// three state in raft
type raftState string

const (
	LEADER    raftState = "LEADER"
	CANDIDATE raftState = "CANDIDATE"
	FOLLOWER  raftState = "FOLLOWER"
	NULL int = -1
)


// about log
type LogEntry struct {
	// first is 1.
	Command string
	Term    int // when entry was received by leader
}

 // random time
func randomTimeForElectionTimeout() time.Duration {
	ms := 400 + (rand.Int63() % 10) //TODO
	return time.Duration(ms) * time.Millisecond
}

func randomTimeForHeartBeat() time.Duration {
	ms := 50 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
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


// Log
type Log struct {
	log2A *log.Logger
	log2B *log.Logger
}

func NewLog() *Log {
	// 删除文件
	_ = os.Remove("./raft.log")
	logFile, err := os.OpenFile("./raft.log", os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println("open log file failed, err:", err)
        return nil
    }
	return &Log{
		log2A: log.New(logFile, "[2A]", log.Lmicroseconds),
		log2B: log.New(logFile, "[2B]", log.Lmicroseconds),
	}
}

func (l *Log) printf2A(me int, state raftState, term int, msg string) {
	l.log2A.Printf("[peer %d | %s | term %d]: %s", me, state, term, msg)
}

// func (l *Log) debug_SendHeartbeat(me int, state raftState, term int, ) {
// 	l.logDEBUG.Printf("")
// }

var logg *Log = NewLog()
