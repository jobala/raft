package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	logs      []EntryLog
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A).
	currTerm atomic.Int32
	votedFor atomic.Int32

	// Your data here (3B, 3C)
	nextIndex   map[int]int
	matchIndex  map[int]int
	commitIndex atomic.Int32
	lastApplied atomic.Int32

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state          atomic.Int32
	votes          atomic.Int32
	eventCh        chan EventMsg
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	voteReqCh      map[int]chan EventMsg
	voteMu         sync.Mutex
	appendEntryCh  map[int]chan EventMsg
	appendMu       sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	return int(rf.currTerm.Load()), State(rf.state.Load()) == Leader
}

func (rf *Raft) wonElection() bool {
	return rf.votes.Load() >= int32(math.Ceil(float64(len(rf.peers))/2.0))
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm.Load())
	e.Encode(rf.votedFor.Load())
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int32
	var votedFor int32
	var logs []EntryLog

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Error decoding state")
	} else {
		rf.currTerm.Store(term)
		rf.votedFor.Store(votedFor)

		rf.logs = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.mu.Unlock()

	evt := EventMsg{
		Type:         RequestVote,
		CandidateId:  args.CandidateId,
		Term:         args.Term,
		RequestId:    rand.Int(),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	}

	ch := make(chan EventMsg)
	rf.voteMu.Lock()
	rf.voteReqCh[evt.RequestId] = ch
	rf.voteMu.Unlock()

	rf.eventCh <- evt

	select {
	case res := <-ch:
		rf.voteMu.Lock()
		reply.Term = res.Term
		reply.VoteGranted = res.VoteGranted
		rf.voteMu.Unlock()
	case <-time.After(100 * time.Millisecond):
		rf.voteMu.Lock()
		reply.Term = int(rf.currTerm.Load())
		reply.VoteGranted = false
		rf.voteMu.Unlock()
	}

	rf.voteMu.Lock()
	delete(rf.voteReqCh, evt.RequestId)
	rf.voteMu.Unlock()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	rf.mu.Unlock()

	evt := EventMsg{
		Type:         AppendEntry,
		LeaderId:     args.LeaderId,
		Term:         args.Term,
		Entries:      args.Entries,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		LeaderCommit: args.LeaderCommit,
		RequestId:    rand.Int(),
	}

	ch := make(chan EventMsg)
	rf.appendMu.Lock()
	rf.appendEntryCh[evt.RequestId] = ch
	rf.appendMu.Unlock()

	rf.eventCh <- evt

	select {
	case res := <-ch:
		rf.appendMu.Lock()
		reply.Term = res.Term
		reply.Success = res.Success
		rf.appendMu.Unlock()
	case <-time.After(100 * time.Millisecond):
		rf.appendMu.Lock()
		reply.Term = int(rf.currTerm.Load())
		reply.Success = false
		rf.appendMu.Unlock()
	}

	rf.appendMu.Lock()
	delete(rf.appendEntryCh, evt.RequestId)
	rf.appendMu.Unlock()
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
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
	isLeader := rf.state.Load() == int32(Leader)
	if !isLeader {
		return 0, 0, false
	}

	rf.mu.Lock()
	rf.logs = append(rf.logs, EntryLog{
		Command: command,
		Term:    int(rf.currTerm.Load()),
	})

	term := int(rf.currTerm.Load())
	lastIdx := len(rf.logs) - 1
	rf.persist()
	rf.mu.Unlock()

	rf.eventCh <- EventMsg{
		Type:    Command,
		Command: command,
	}

	return lastIdx, term, isLeader
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

func (rf *Raft) startElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	timeout := time.Duration(rand.Intn(ElectionTimeout)+200) * time.Millisecond

	rf.electionTimer = time.AfterFunc(timeout, func() {
		rf.eventCh <- EventMsg{Type: StartElection}
	})
}

func (rf *Raft) ReplicateLog() {
	rf.mu.Lock()
	DPrintf("leader %d: %v", rf.me, rf.logs)
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if State(rf.state.Load()) != Leader {
			return
		}

		go func(peer int) {
			reply := AppendEntryReply{}
			args := rf.getAppendArgs(peer)
			ok := rf.sendAppendEntry(peer, &args, &reply)

			DPrintf("(Leader %d ) replicating to Follower %d. Follower rejected entries, retrying... follower is down %v", rf.me, peer, ok)
			// if reply is unsuccessful, reduce nextIndex and retry
			for ok && !reply.Success {
				if State(rf.state.Load()) != Leader {
					return
				}
				rf.mu.Lock()
				rf.nextIndex[peer] -= 1
				rf.mu.Unlock()

				reply = AppendEntryReply{}
				args := rf.getAppendArgs(peer)
				ok = rf.sendAppendEntry(peer, &args, &reply)

				if ok && !reply.Success {
					DPrintf("(Leader %d Term %d) replicating to Follower %d failed. Follower rejected entries, retrying... follower is down %v", rf.me, rf.currTerm.Load(), peer, !ok)
				} else if ok && reply.Success {
					DPrintf("(Leader %d Term %d) replicated  entries to Follower %d successfully entries: %v", rf.me, rf.currTerm.Load(), peer, args.Entries)
				}

				rf.eventCh <- EventMsg{
					Type:         AppendEntryResponse,
					Term:         reply.Term,
					From:         peer,
					Success:      reply.Success,
					EntryCount:   len(args.Entries),
					PrevLogIndex: args.PrevLogIndex,
					PrevLogTerm:  args.PrevLogTerm,
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}
}

func (rf *Raft) getAppendArgs(peer int) AppendEntryArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.nextIndex[peer] <= 0 {
		rf.nextIndex[peer] = 1 // Can't go below first real entry
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	entries := rf.logs[rf.nextIndex[peer]:]

	return AppendEntryArgs{
		LeaderId:     rf.me,
		Term:         int(rf.currTerm.Load()),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: int(rf.commitIndex.Load()),
	}
}

func (rf *Raft) startHearbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.heartbeatTimer = time.AfterFunc(HeartbeatTimer, func() {
		if rf.killed() {
			return
		}

		go func() {
			if State(rf.state.Load()) == Leader {
				for i := range rf.peers {
					if i == rf.me {
						continue
					}

					go func(peer int) {
						reply := AppendEntryReply{}
						args := rf.getAppendArgs(peer)
						if ok := rf.sendAppendEntry(peer, &args, &reply); ok {
							rf.eventCh <- EventMsg{
								Type:         AppendEntryResponse,
								Term:         reply.Term,
								From:         peer,
								Success:      reply.Success,
								PrevLogIndex: args.PrevLogIndex,
								PrevLogTerm:  args.PrevLogTerm,
								EntryCount:   len(args.Entries),
							}
						}

					}(i)
				}

			}

			rf.startHearbeat()
		}()

	})

}

func (rf *Raft) requestVoteFromPeers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			lastLogIdx := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIdx].Term
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			args := RequestVoteArgs{
				CandidateId:  rf.me,
				Term:         int(rf.currTerm.Load()),
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}

			if ok := rf.sendRequestVote(peer, &args, &reply); ok {
				rf.eventCh <- EventMsg{
					Type:        RequestVoteResponse,
					From:        peer,
					VoteGranted: reply.VoteGranted,
					Term:        reply.Term,
				}
			}

		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		event := <-rf.eventCh
		rf.handleEvent(event)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex.Load() > rf.lastApplied.Load() {
			rf.lastApplied.Add(1)
			entry := rf.logs[rf.lastApplied.Load()]
			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				Command:      entry.Command,
				CommandIndex: int(rf.lastApplied.Load()),
				CommandValid: true,
			}

			rf.mu.Lock()
		}
		rf.mu.Unlock()
		// Wait for new commits (use condition variable or sleep)
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			DPrintf("Leader(me) matchIndex: %d", rf.matchIndex[i])
			continue
		}
		DPrintf("Follower %d matchIndex: %d", i, rf.matchIndex[i])
	}

	for N := rf.commitIndex.Load() + 1; N < int32(len(rf.logs)); N++ {
		if rf.logs[N].Term != int(rf.currTerm.Load()) {
			continue
		}

		count := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= int(N) {
				count++
			}
		}

		majority := len(rf.peers)/2 + 1

		if count >= majority {
			rf.commitIndex.Store(N)
		} else {
			break
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.eventCh = make(chan EventMsg)
	rf.votedFor.Store(-1)
	rf.currTerm.Store(0)
	rf.state.Store(int32(Follower))
	rf.startElectionTimer()
	rf.voteReqCh = map[int]chan EventMsg{}
	rf.appendEntryCh = map[int]chan EventMsg{}
	rf.matchIndex = map[int]int{}
	rf.nextIndex = map[int]int{}
	rf.logs = []EntryLog{
		{Term: 0, Command: 0},
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
