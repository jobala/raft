package raft

func (rf *Raft) handleEvent(evt EventMsg) {
	rf.mu.Lock()
	term := int(rf.currTerm.Load())
	rf.mu.Unlock()

	if term < evt.Term {
		rf.becomeFollower(evt.Term)
		rf.handleFollowerEvt(evt)
		return
	}

	if evt.Type == AppendEntry && term > evt.Term {
		rf.appendMu.Lock()
		evt.Term = term
		evt.Success = false
		rf.appendEntryCh[evt.RequestId] <- evt
		rf.appendMu.Unlock()
		return
	}

	state := State(rf.state.Load())

	switch state {
	case Follower:
		rf.handleFollowerEvt(evt)
	case Candidate:
		rf.handleCandidateEvt(evt)
	case Leader:
		rf.handleLeaderEvt(evt)
	}
}

func (rf *Raft) handleFollowerEvt(evt EventMsg) {
	switch evt.Type {
	case RequestVote:
		if int(rf.currTerm.Load()) > evt.Term {
			evt.VoteGranted = false
			evt.Term = int(rf.currTerm.Load())
			rf.voteMu.Lock()
			rf.voteReqCh[evt.RequestId] <- evt
			rf.voteMu.Unlock()
			return
		}

		rf.mu.Lock()
		voterLastLogIdx := len(rf.logs) - 1
		voterLastLogTerm := rf.logs[voterLastLogIdx].Term

		candidateIsUptoDate := evt.LastLogTerm > voterLastLogTerm || (evt.LastLogTerm == voterLastLogTerm && evt.LastLogIndex >= voterLastLogIdx)
		// DPrintf("vote request: LastLogIndex: %v LastLogTerm: %v from candidate %d", evt.LastLogIndex, evt.LastLogTerm, evt.CandidateId)
		// DPrintf("voter %d      LastLogIndex: %v LastLogTerm: %v", rf.me, voterLastLogIdx, voterLastLogTerm)
		rf.mu.Unlock()
		if int(rf.currTerm.Load()) == evt.Term && (rf.votedFor.Load() == int32(evt.CandidateId) || rf.votedFor.Load() == -1) && candidateIsUptoDate {
			rf.startElectionTimer()
			rf.votedFor.Store(int32(evt.CandidateId))
			rf.currTerm.Store(int32(evt.Term))
			rf.persist()

			rf.voteMu.Lock()
			evt.VoteGranted = true
			evt.Term = int(rf.currTerm.Load())
			rf.voteReqCh[evt.RequestId] <- evt
			rf.voteMu.Unlock()

			DPrintf("follower %d  voted for candidate %d", rf.me, evt.CandidateId)
		} else {
			rf.voteMu.Lock()
			evt.VoteGranted = false
			evt.Term = int(rf.currTerm.Load())
			rf.voteReqCh[evt.RequestId] <- evt
			rf.voteMu.Unlock()

			DPrintf("follower %d refused to vote for candidate %d", rf.me, evt.CandidateId)
		}

	case AppendEntry:
		rf.startElectionTimer()
		rf.mu.Lock()
		rf.currTerm.Store(int32(evt.Term))
		rf.persist()

		DPrintf("Follower %d: received AppendEntry from Leader %d with Entries %d and CommitIndex %d, leader's term: %d",
			rf.me, evt.LeaderId, len(evt.Entries), evt.LeaderCommit, evt.Term)
		// DPrintf("Follower %d: Logs %v, EvtPrevLogIndex %d, EvtPrevLogTerm %d", rf.me,
		// rf.logs, evt.PrevLogIndex, evt.PrevLogTerm)

		appendAccepted := false

		if evt.PrevLogIndex < len(rf.logs) && rf.logs[evt.PrevLogIndex].Term == evt.PrevLogTerm {
			appendAccepted = true
			insertIndex := evt.PrevLogIndex + 1

			for i, newEntry := range evt.Entries {
				logIndex := insertIndex + i
				if logIndex < len(rf.logs) {
					if rf.logs[logIndex].Term != newEntry.Term {
						rf.logs = rf.logs[:logIndex]
						break
					}
				} else {
					break
				}
			}

			newEntries := evt.Entries
			if insertIndex < len(rf.logs) {
				skipCount := len(rf.logs) - insertIndex
				if skipCount < len(evt.Entries) {
					newEntries = evt.Entries[skipCount:]
				} else {
					newEntries = []EntryLog{}
				}
			}

			rf.logs = append(rf.logs, newEntries...)
			rf.persist()
			newCommitIndex := min(evt.LeaderCommit, len(rf.logs)-1)
			rf.commitIndex.Store(int32(newCommitIndex))
		} else {
			DPrintf("Follower %d rejected entries EvtPrevTerm %d EvtLastLogIdx %v", rf.me, evt.PrevLogTerm, evt.PrevLogIndex)
			DPrintf("Follower %d expected (evt.PrevLogIndex %d < len(rf.logs) %d )", rf.me, evt.PrevLogIndex, len(rf.logs))
			if evt.PrevLogIndex < len(rf.logs) {
				DPrintf("Also expected TermInLog %d == TermInEvt %d", rf.logs[evt.PrevLogIndex].Term, evt.PrevLogTerm)
			}
			DPrintf("================================")
		}
		evt.Success = appendAccepted
		evt.Term = int(rf.currTerm.Load())
		rf.mu.Unlock()

		rf.appendMu.Lock()
		rf.appendEntryCh[evt.RequestId] <- evt
		rf.appendMu.Unlock()
	case StartElection:
		rf.startElection()
	}
}

func (rf *Raft) handleCandidateEvt(evt EventMsg) {
	switch evt.Type {
	case StartElection:
		rf.startElection()

	case RequestVote:
		DPrintf("Candidate %d received a vote request", rf.me)
		rf.voteMu.Lock()
		evt.Term = int(rf.currTerm.Load())
		evt.VoteGranted = false
		rf.voteReqCh[evt.RequestId] <- evt
		rf.voteMu.Unlock()

	case RequestVoteResponse:
		if rf.currTerm.Load() == int32(evt.Term) && evt.VoteGranted {
			DPrintf("candidate %d received vote from follower %d", rf.me, evt.From)
			rf.votes.Add(1)
		}

		if rf.wonElection() {
			DPrintf("-> candidate %d won the election <-", rf.me)
			rf.becomeLeader()
		}

	case AppendEntry:
		DPrintf("Candiate %d received append entry", rf.me)
		if rf.currTerm.Load() > int32(evt.Term) {
			evt.Term = int(rf.currTerm.Load())
			evt.Success = false
			DPrintf("Candidate %d telling leader of new term %d", rf.me, evt.Term)

			rf.appendMu.Lock()
			rf.appendEntryCh[evt.RequestId] <- evt
			rf.appendMu.Unlock()
		} else {
			rf.becomeFollower(evt.Term)
			rf.handleFollowerEvt(evt)
		}
	}
}

func (rf *Raft) handleLeaderEvt(evt EventMsg) {
	switch evt.Type {
	case RequestVote:
		DPrintf("Leader %d received a vote request", rf.me)
	case AppendEntryResponse:
		if rf.currTerm.Load() < int32(evt.Term) {
			DPrintf("Leader %d is stepping down", rf.me)
			rf.becomeFollower(evt.Term)
		}

		if evt.Success {

			DPrintf("Leader %d AppendEntry Success %v, From %d updating nextIndex", rf.me, evt.Success, evt.From)
			DPrintf("Leader %d: Updating matchIndex[%d] from %d to %d (PrevLogIndex=%d, EntryCount=%d)",
				rf.me, evt.From, rf.matchIndex[evt.From], evt.PrevLogIndex+evt.EntryCount,
				evt.PrevLogIndex, evt.EntryCount)
			rf.mu.Lock()
			rf.matchIndex[evt.From] = evt.PrevLogIndex + evt.EntryCount
			rf.nextIndex[evt.From] = rf.matchIndex[evt.From] + 1
			rf.mu.Unlock()

			rf.Commit()
		}

	case Command:
		rf.ReplicateLog()
	}
}

func (rf *Raft) startElection() {
	rf.startElectionTimer()

	rf.state.Store(int32(Candidate))
	rf.currTerm.Add(1)
	rf.votedFor.Store(int32(rf.me))
	rf.requestVoteFromPeers()
	rf.votes.Store(1)
	rf.persist()

	DPrintf("peer %d is starting an election", rf.me)
}

func (rf *Raft) becomeLeader() {
	rf.startHearbeat()
	rf.electionTimer.Stop()
	rf.state.Store(int32(Leader))

	rf.mu.Lock()
	for peer := range rf.peers {
		rf.nextIndex[peer] = 1
		rf.matchIndex[peer] = 0
	}
	rf.mu.Unlock()

	DPrintf("candidate %d became a leader", rf.me)
}

func (rf *Raft) becomeFollower(newerTerm int) {
	rf.startElectionTimer()
	DPrintf("peer %d is stepping down from %s to follower", rf.me, State(rf.state.Load()))

	if rf.heartbeatTimer != nil {
		rf.heartbeatTimer.Stop()
	}

	rf.state.Store(int32(Follower))
	rf.currTerm.Store(int32(newerTerm))
	rf.votedFor.Store(-1)
	rf.persist()
}
