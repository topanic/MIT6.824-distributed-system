package raft

import (
	"log"
	"math/rand"
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



// 
// about time
const (
	TickInterval int64 = 30
	HeartbeatInterval int64 = 100
	
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

func (rf *Raft) setHeartbeatTimeout(d time.Duration) {
	t := time.Now()
	t = t.Add(d)
	rf.heartbeatTime = t
}

func (rf *Raft) setElectionTimeout(d time.Duration) {
	t := time.Now()
	t = t.Add(d)
	rf.electionTime = t
}



// about log
type LogEntries []LogEntry

type LogEntry struct {
	// first is 1.
	Command interface{}
	Term    int // when entry was received by leader
}

// index and term start from 1
func (les LogEntries) getEntry(index int) *LogEntry {
	if index < 0 {
		log.Panic("LogEntries.getEntry: index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term: 0,
		}
	}
	if index > len(les) {
		return &LogEntry{
			Command: nil,
			Term: NULL,
		}
	}
	return &les[index - 1]
}

func (les LogEntries) lastLogInfo() (index, term int) {
	index = len(les)
	lastEntry := les.getEntry(index)
	return index, lastEntry.Term
}

func (les LogEntries) getSlice(startIndex, endIndex int) LogEntries  {
	if startIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(les))
		log.Panic("LogEntries.getSlice: startIndex out of range. \n")
	}
	if endIndex > len(les)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(les))
		log.Panic("LogEntries.getSlice: endIndex out of range.\n")
	}
	if startIndex > endIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panic("LogEntries.getSlice: startIndex > endIndex.\n")
	}
	return les[startIndex-1 : endIndex-1]
}



// For 2B
func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed msgs to apply, and apply them only after unlocked
		appliedMsgs := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
			Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}