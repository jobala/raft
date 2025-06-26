package raft

import (
	"time"
)

type EventType int
type State int

const ElectionTimeout = 400
const HeartbeatTimer = 200 * time.Millisecond

const (
	RequestVote EventType = iota
	RequestVoteResponse
	AppendEntry
	AppendEntryResponse
	StartElection
	Command
)

func (e EventType) String() string {
	return []string{"RequestVote", "RequestVoteResponse", "AppendEntry", "AppendEntryResponse", "StartElection", "Command"}[e]
}

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return []string{"Follower", "Candidate", "Leader"}[s]
}

type EventMsg struct {
	Type         EventType
	To           int
	From         int
	Term         int
	CandidateId  int
	RequestId    int
	Command      any
	VoteGranted  bool
	LeaderId     int
	Success      bool
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []EntryLog
	EntryCount   int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	LeaderId     int
	Term         int
	Entries      []EntryLog
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type EntryLog struct {
	Term    int
	Command any
}
