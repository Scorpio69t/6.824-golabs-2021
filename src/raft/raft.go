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

	"errors"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"github.com/google/uuid"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

// Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Logger.
	logger *raftLogger

	// Your data here (2A, 2B, 2C).
	leaderId int
	raftState
	lastContact      time.Time
	lastContactMutex sync.RWMutex
	logEntries       []*LogEntry

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Channels for communication
	rpcCh         chan *RPC
	killCh        chan struct{}
	applyCh       chan ApplyMsg
	startCh       chan *StartCall
	applyNotifyCh chan bool
}

// pushLogToLocal append a new entry to local logEntries.
// The function is not thread-safe, and it is necessary to accquire lastLock before being called.
func (r *Raft) pushLogToLocal(log *LogEntry) {
	r.logger.Info("pushToLocal: oldIndex=%d, oldTerm=%d, newIndex=%d, newTerm=%d\n", r.lastLogIndex, r.lastLogTerm, log.Index, log.Term)
	r.logEntries = append(r.logEntries, log)
}

func (r *Raft) LastContact() time.Time {
	r.lastContactMutex.RLock()
	last := r.lastContact
	r.lastContactMutex.RUnlock()
	return last
}

func (r *Raft) SetLastContact() {
	r.lastContactMutex.Lock()
	r.lastContact = time.Now()
	r.lastContactMutex.Unlock()
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(r.raftState.getCurrentTerm()), r.raftState.getState() == Leader
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state.
//
func (r *Raft) readPersist(data []byte) {
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (r *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs is the RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r *Raft) handleRPC(args interface{}, rpcId string) (response RPCResponse) {
	// r.Trace("RPC: handleRPC was called\n")
	// defer r.Trace("RPC: handleRPC returned\n")

	rpc := &RPC{
		Args:   args,
		respCh: make(chan RPCResponse),
		id:     rpcId,
	}
	defer close(rpc.respCh)

	// We use the concurrent model based on channel, and
	// the actual handler will excute in r.run().
	r.logger.Trace("handleRPC(%s): before sending to r.rpcCh", rpc.id)
	r.rpcCh <- rpc
	r.logger.Trace("handleRPC(%s): after sending to r.rpcCh", rpc.id)

	resp := <-rpc.respCh
	return resp
}

func (r *Raft) processRPC(rpc *RPC) {
	r.logger.Trace("processRPC(%s): called\n", rpc.id)
	switch args := rpc.Args.(type) {
	case *RequestVoteArgs:
		r.requestVote(rpc, args)
	case *AppendEntriesArgs:
		r.appendEntries(rpc, args)
	default:
		r.logger.Error("Unknown RPC type.\n")
	}
}

// RequestVote
// example RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rpcId := uuid.NewString()
	r.logger.Debug("RPC: RequestVote(%s): called: candidate=%d, term=%d\n", rpcId, args.CandidateId, args.Term)
	defer func() {
		r.logger.Debug("RPC: RequestVote(%s): returned: candidate=%d, term=%d, granted=%d\n", rpcId, args.CandidateId, reply.Term, reply.VoteGranted)
	}()

	response := r.handleRPC(args, rpcId)
	reply.Term = response.Response.(*RequestVoteReply).Term
	reply.VoteGranted = response.Response.(*RequestVoteReply).VoteGranted
}

func (r *Raft) requestVote(rpc *RPC, args *RequestVoteArgs) {
	// r.Debug("RPC: enter requestVote\n")
	// defer r.Debug("RPC: leave requestVote\n")

	resp := &RequestVoteReply{}
	var err error
	defer func() {
		rpc.Response(resp, err)
	}()

	// Leader already existed.
	if r.leaderId != -1 && r.leaderId != args.CandidateId {
		r.logger.Warn("RPC requestVote: Leader existed. leaderId=%d, candidateId=%d", r.leaderId, args.CandidateId)
		return
	}

	// Ignore older term.
	if args.Term < int(r.raftState.getCurrentTerm()) {
		return
	}

	// Discover newer term, convert to follower and update term.
	if args.Term > int(r.raftState.getCurrentTerm()) {
		r.logger.Info("RPC requestVote: discover newer term.")
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
	}

	// Have voted in this term.
	if r.raftState.LastVoteTerm() == uint64(args.Term) && r.raftState.LastVoteFor() != -1 {
		r.logger.Warn("RPC requestVote: duplicated RequestVote term, term=%d\n", args.Term)
		if r.raftState.LastVoteFor() == int32(args.CandidateId) {
			resp.VoteGranted = true
		}
		return
	}

	// Compare lastTerm and lastIndex
	lastIndex, lastTerm := r.raftState.getLastLog()
	if lastTerm > uint64(args.LastLogTerm) {
		r.logger.Info("RPC requestVote: our lastTerm is greater.\n")
		return
	}
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		r.logger.Info("RPC requestVote: our term is equal to that of args, but the lastIndex is greater.\n")
		return
	}

	// Grant votes for the candidate.
	r.logger.Info("RPC requestVote: vote for candidate=%d, term=%d\n", args.CandidateId, args.Term)
	resp.VoteGranted = true
	resp.Term = args.Term
	r.raftState.SetLastVoteTerm(r.raftState.getCurrentTerm())
	r.raftState.SetLastVoteFor(int32(args.CandidateId))
	r.SetLastContact()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcId := uuid.NewString()
	r.logger.Debug("RPC: AppendEntries(%s): called: leader=%d, term=%d, prevLogIndex=%d, leaderCommit=%d, entries=%s\n",
		rpcId, args.LeaderId, args.Term, args.PrevLogIndex, args.LeaderCommit, formatLogEntries(args.Entries))
	defer func() {
		r.logger.Debug("RPC: AppendEntries(%s): returned: term=%d, success=%d", rpcId, reply.Term, reply.Success)
	}()
	response := r.handleRPC(args, rpcId)
	reply.Success = response.Response.(*AppendEntriesReply).Success
	reply.Term = response.Response.(*AppendEntriesReply).Term
}

// updateLastLog updates r.lastLogIndex and r.lastLogTerm
// The function is not thread-safe.
func (r *Raft) updateLastLog() {
	newLastLog := r.logEntries[len(r.logEntries)-1]
	r.lastLogIndex = newLastLog.Index
	r.lastLogTerm = newLastLog.Term
	r.logger.Debug("updateLastLog: index=%d, term=%d\n", newLastLog.Index, newLastLog.Term)
}

// deleteLogAfter delete conflict logs after index for the followers.
// The function is not thread-safe.
func (r *Raft) deleteLogAfter(index uint64) {
	r.logger.Warn("delete: conflictIndex=%d\n", index)
	r.logEntries = r.logEntries[:index]
}

// appendEntries implements the AppendEntries RPC in the paper.
func (r *Raft) appendEntries(rpc *RPC, args *AppendEntriesArgs) {
	// r.Trace("appendEntries: called\n")
	// defer func() {
	// r.Trace("appendEntries: returned\n")
	// }()

	// Defer return rpc.
	resp := &AppendEntriesReply{
		Term:    int(r.raftState.getCurrentTerm()),
		Success: false,
	}
	var err error
	defer func() {
		rpc.Response(resp, err)
	}()

	// Discover older leader, return directly.
	if r.raftState.getCurrentTerm() > uint64(args.Term) {
		r.logger.Warn("RPC: appendEntries: receive older term\n")
		// Reply false.
		return
	}

	// Discover newer leader, convert to follower.
	if r.me != args.LeaderId && (r.raftState.getCurrentTerm() < uint64(args.Term) || r.raftState.getState() != Follower) {
		r.logger.Info("RPC: appendEntries: find new leader: leader=%d, term=%d\n", args.LeaderId, args.Term)
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
		resp.Term = args.Term
	}

	// This RPC is valid, so the follower should admit the leader.
	// Update r.lastContact and r.leaderId.
	r.SetLastContact()
	r.leaderId = args.LeaderId

	// Operations below will read/write lastIndex, and logEntries, we need to accquire the lastLock.
	// r.Trace("appendEntries: before r.lastLock.Lock()\n")
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	// r.Trace("appendEntries: after r.lastLock.Lock()\n")

	// The lastIndex or lastTerm may not up-to-date so
	// we need to update then first.
	r.updateLastLog()

	// Do not contain an entry at prevLogIndex whose term matches prevLogTerm.
	if r.lastLogIndex < args.PrevLogIndex {
		r.logger.Info("RPC: appendEntries: do not contain an entry at prevLogIndex: prevLogIndex=%d\n", args.PrevLogIndex)
		// Reply false.
		return
	}
	if r.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Delete conflict entries.
		r.logger.Info("RPC: appendEntries: prevLogIndex conflict\n")
		r.deleteLogAfter(args.PrevLogIndex)
		r.updateLastLog()
		// Reply false.
		return
	}

	// The entry at prevLogIndex has the same term as prevLogTerm, so we will reply true below.
	// Append any new entries not already in the log.
	newEntries := []*LogEntry{}
	for i, newEntry := range args.Entries {
		if r.lastLogIndex >= newEntry.Index {
			if r.logEntries[r.lastLogIndex].Term != newEntry.Term {
				// Find conflict logs at index=newEntry.Index.
				r.logger.Debug("RPC: appendEntries: conflict entries after prevLogIndex: conflictIndex=%d\n", newEntry.Index)
				// Delete r.logEntries[newEntry.Index:].
				r.deleteLogAfter(newEntry.Index)
				r.updateLastLog()
				// We should append entries whose index is equal or greater than newEntry.Index(conflict one).
				newEntries = args.Entries[i:]
				break
			} else {
				// Skip duplicated logs.
				continue
			}
		} else {
			// No conflict entries found. Append new entries.
			newEntries = args.Entries[i:]
			break
		}
	}
	for _, newEntry := range newEntries {
		r.pushLogToLocal(newEntry)
	}
	r.updateLastLog()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if r.getCommitIndex() < args.LeaderCommit {
		r.logger.Info("RPC: appendEntries(%s): attempt to update commitIndex\n", rpc.id)
		r.setCommitIndex(min(args.LeaderCommit, r.lastLogIndex))
		r.applyNotifyCh <- true
	}
	resp.Success = true
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.Trace("RPC: sendAppendEntries, server=%d, commitIndex=%d\n", server, args.LeaderCommit)
	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		r.logger.Warn("RPC: sendAppendEntries failed: server=%d, commitIndex=%d\n", server, args.LeaderCommit)
	}
	return ok
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
//
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	r.logger.Debug("RPC: sendRequestVote, server=%d, term=%d\n", server, args.Term)
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		r.logger.Warn("RPC: sendRequestVote failed: server=%d, term=%d\n", server, args.Term)
	}
	return ok
}

//
// Start
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
//
func (r *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	defer func() {
		r.logger.Debug("Start: returned. command=%v, index=%d, term=%d, isLeader=%d\n", command, index, term, isLeader)
	}()

	call := &StartCall{
		command: command,
		respCh:  make(chan *StartResult),
	}
	defer close(call.respCh)

	r.startCh <- call
	resp := <-call.respCh
	index, term, isLeader = int(resp.Index), int(resp.Term), resp.IsLeader
	return
}

//
// Kill kills the Raft service.
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
	r.logger.Error("Kill\n")
	r.setState(Shutdown)
	r.killCh <- struct{}{}
}

func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
}

// run is the main running goroutine.
func (r *Raft) run() {
	defer func() {
		r.logger.Error("run returned\n")
	}()
	for !r.killed() {
		r.logger.Debug("run: new round\n")
		select {
		case <-r.killCh:
			// Clear the leaderID.
			r.leaderId = -1
			return
		default:
			// No operation.
		}

		// Run a sub FSM
		switch r.raftState.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		default:
			r.logger.Debug("run: unknown state, return.\n")
			return
		}
	}
}

func (r *Raft) runFollower() {
	r.logger.Debug("runFollower: called\n")
	heartbeatTimeout := randomIntBetween(followerMinElectionTimeoutMs, followerMaxElectionTimeoutMs)
	heartbeatTimer := time.After(time.Millisecond * time.Duration(heartbeatTimeout))

	// A long loop for follower.
	for r.raftState.getState() == Follower {
		select {
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.logger.Trace("RPC(%s)<-r.rpcCh\n", rpc.id)
			r.processRPC(rpc)
		case <-heartbeatTimer:
			// Clear the timeout.
			oldTimeout := heartbeatTimeout
			heartbeatTimeout = randomIntBetween(followerMinElectionTimeoutMs, followerMaxElectionTimeoutMs)
			heartbeatTimer = time.After(time.Millisecond * time.Duration(heartbeatTimeout))

			// Success.
			if time.Now().Sub(r.LastContact()) < time.Millisecond*time.Duration(oldTimeout) {
				continue
			}

			// Switch to the candidate state.
			r.leaderId = -1
			r.setCurrentTerm(r.raftState.getCurrentTerm() + 1)
			r.setState(Candidate)
			return
		case sc := <-r.startCh:
			sc.respCh <- &StartResult{
				Index:    r.getLastIndex() + 1,
				Term:     r.getCurrentTerm(),
				IsLeader: false,
			}
		}
	}

}

func (r *Raft) setupCandidate() (voteCh chan *RequestVoteReply) {
	// Vote channel.
	voteCh = make(chan *RequestVoteReply, 5)

	// Vote for self.
	voteCh <- &RequestVoteReply{
		Term:        int(r.raftState.currentTerm),
		VoteGranted: true,
	}
	r.raftState.SetLastVoteFor(int32(r.me))
	r.raftState.SetLastVoteTerm(r.raftState.currentTerm)

	// Send RequestVote RPC
	r.lastLock.Lock()
	lastLog := r.logEntries[len(r.logEntries)-1]
	r.lastLock.Unlock()
	args := &RequestVoteArgs{
		Term:         int(r.raftState.getCurrentTerm()),
		CandidateId:  r.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for peer := range r.peers {
		// Skip me.
		if peer == r.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := r.sendRequestVote(server, args, reply)
			if ok {
				voteCh <- reply
			}
		}(peer)
	}

	return
}

func (r *Raft) runCandidate() {
	r.logger.Debug("runCandidate: start election: term=%d\n", r.raftState.getCurrentTerm())
	electionTimeout := randomTimeout(time.Millisecond * candidateElectionMs)

	voteCh := r.setupCandidate()
	defer func() {
		r.logger.Debug("runCandidate: returned\n")
	}()

	// Votes counter.
	grantedVotes := 0
	neededVotes := (len(r.peers) + 1) / 2

	// A long loop for waiting votes.
	for r.raftState.getState() == Candidate {
		select {
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case <-electionTimeout:
			// Increase term and start a new round of election.
			r.logger.Warn("election: timeout\n")
			r.setCurrentTerm(r.raftState.getCurrentTerm() + 1)
			return
		case reply := <-voteCh:
			// Find higher term, become the follower state.
			if reply.Term > int(r.raftState.currentTerm) {
				r.setCurrentTerm(uint64(reply.Term))
				r.setState(Follower)
				return
			}

			// Receive grantedVote.
			if reply.VoteGranted {
				grantedVotes++
				if grantedVotes >= neededVotes {
					// Switch to the leader state.
					r.logger.Warn("election: won, term=%d\n", r.raftState.getCurrentTerm())
					r.leaderId = r.me
					r.setState(Leader)
					return
				}
			}
		case sc := <-r.startCh:
			sc.respCh <- &StartResult{
				Index:    r.getLastIndex() + 1,
				Term:     r.getCurrentTerm(),
				IsLeader: false,
			}
		}
	}
}

var (
	ErrorSendToMe         = errors.New("Send to me.")
	ErrorInvalidServer    = errors.New("Invalid server id.")
	ErrorFollowerUpToDate = errors.New("Follower up to date.")
)

// newAppendEntriesArgs build AppendEntries RPC request according to the target server.
func (r *Raft) newAppendEntriesArgs(server int, isHeartbeat bool) (args *AppendEntriesArgs, err error) {
	// Send to self
	if server == r.me {
		err = ErrorSendToMe
		return
	}

	// Send heartbeat
	if isHeartbeat {
		args = &AppendEntriesArgs{
			Term:     int(r.raftState.getCurrentTerm()),
			LeaderId: r.me,
			Entries:  nil,
		}
		return
	}

	// Invalid server
	if server < 0 || server >= len(r.peers) {
		err = ErrorInvalidServer
		return
	}

	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	r.indexLock.RLock()
	defer r.indexLock.RUnlock()

	// Valid AppendEntries RPC.
	// Deep copy newEntries.
	serverNextIndex := r.nextIndex[server]
	end := min(uint64(len(r.logEntries)), serverNextIndex+maxLenNewEntries)
	length := end - r.nextIndex[server]
	nextEntries := make([]*LogEntry, length)
	sourceEntries := r.logEntries[serverNextIndex:end]
	for i := range nextEntries {
		nextEntries[i] = new(LogEntry)
		*nextEntries[i] = *sourceEntries[i]
	}
	prevLog := r.logEntries[serverNextIndex-1]
	args = &AppendEntriesArgs{
		Term:         int(r.getCurrentTerm()),
		LeaderId:     r.me,
		LeaderCommit: r.commitIndex,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      nextEntries,
	}

	return
}

type appendEntriesReplyWrapper struct {
	followerId int
	reply      *AppendEntriesReply
}

func (r *Raft) setupLeader(lc *leaderChannels) {
	// Reinitialize.
	for i := range r.peers {
		r.nextIndex[i] = r.lastLogIndex + 1
		r.matchIndex[i] = 0
	}
	r.leaderId = r.me

	// Send heartbeats periodically.
	go func() {
		for r.getState() == Leader {
			lc.heartbeatTimerCh <- true
			time.Sleep(leaderAppendEntriesMs * time.Millisecond)
		}
	}()

	// Send AppendEntries RPC periodically.
	go func() {
		for r.getState() == Leader {
			lc.appendEntriesTimerCh <- true
			time.Sleep(leaderAppendEntriesMs * time.Millisecond)
		}
	}()

	// Update commitIndex periodically.
	go func() {
		for r.getState() == Leader {
			lc.updateCommitIndexTimerCh <- true
			time.Sleep(300 * time.Millisecond)
		}
	}()

	return
}

// sendAppendEntriesToAll is called by the leader to send Heartbeat RPC to other peers.
func (r *Raft) sendHeartbeatToAll(lc *leaderChannels) {
	for peer := range r.peers {
		// Skip me.
		if peer == r.me {
			continue
		}

		// Build args
		args, err := r.newAppendEntriesArgs(peer, true)
		switch err {
		case ErrorSendToMe:
			continue
		case ErrorFollowerUpToDate:
			// Do nothing.
		default:
			r.logger.Warn("Follower %d: "+err.Error(), peer)
		}

		// Run a goroutine to send RPC
		go func(server int) {
			reply := &AppendEntriesReply{}
			ok := r.sendAppendEntries(server, args, reply)

			if ok && r.getState() == Leader && reply.Term > int(r.getCurrentTerm()) {
				select {
				case <-lc.ctx.Done():
					return
				default:
					lc.replyCh <- &appendEntriesReplyWrapper{
						followerId: server,
						reply:      reply,
					}
				}
			}
		}(peer)
	}
}

// sendAppendEntriesToAll is called by the leader to send AppendEntries RPC to other peers.
func (r *Raft) sendAppendEntriesToAll(lc *leaderChannels) {
	for peer := range r.peers {
		// Skip me.
		if peer == r.me {
			continue
		}

		// Build args
		args, err := r.newAppendEntriesArgs(peer, false)
		if err == ErrorSendToMe {
			r.logger.Info("Follower %d: "+err.Error(), peer)
			continue
		} else if err != nil {
			r.logger.Warn("Follower %d: "+err.Error(), peer)
		}

		// Run a goroutine to send RPC
		go func(server int) {
			reply := &AppendEntriesReply{}
			ok := r.sendAppendEntries(server, args, reply)

			if ok && r.getState() == Leader {
				select {
				case <-lc.ctx.Done():
					return
				default:
					lc.replyCh <- &appendEntriesReplyWrapper{
						followerId: server,
						reply:      reply,
					}
				}
			}
		}(peer)
	}
}

// updateLeaderCommit updates leader's commitIndex and apply logs.
// The function is only called in r.runLeader().
func (r *Raft) updateLeaderCommit() {
	// For each N, where N is in [commitIndex + 1, lastIndex], and log[N].Term == currentTerm,
	// we set commitIndex = N if a majority of matchIndex[i] >= N.
	var newCommitIndex uint64 = 0
	neededMatch := (len(r.peers) + 1) / 2

	// We use binary search to find N.
	r.indexLock.RLock()
	left := r.commitIndex + 1
	right := r.getLastIndex()
	for left <= right {
		mid := left + (right-left)/2
		// Check term.
		if r.logEntries[mid].Term > r.getCurrentTerm() {
			r.logger.Error("Goroutine::UpdateCommitIndex(): newer term is found.")
			r.indexLock.Unlock()
			return
		}
		if r.logEntries[mid].Term < r.getCurrentTerm() {
			left = mid + 1
			continue
		}

		// Count matched.
		matched := 0
		for _, mi := range r.matchIndex {
			if mi >= mid {
				matched++
			}
		}
		if matched >= neededMatch {
			newCommitIndex = max(newCommitIndex, mid)
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	r.indexLock.RUnlock()

	// Update r.commitIndex.
	if newCommitIndex != 0 {
		r.indexLock.Lock()
		r.setCommitIndex(newCommitIndex)
		r.applyNotifyCh <- true
		r.indexLock.Unlock()
	}
}

func (r *Raft) runLeader() {
	r.logger.Debug("runLeader: called\n")

	// Reinitialize and send AppendEntries RPC periodically.
	lc := newLeaderChannel()
	r.setupLeader(lc)

	defer func() {
		r.logger.Debug("runLeader: before lc.Close()\n")
		lc.cancel()
		r.logger.Debug("runLeader: after lc.Close()\n")
	}()

	for r.raftState.getState() == Leader {
		select {
		case wrapper, ok := <-lc.replyCh:
			if !ok {
				return
			}
			reply := wrapper.reply
			// Discover newer term, convert to follower.
			if reply.Term > int(r.getCurrentTerm()) {
				r.logger.Info("find higher term, leader(%d) -> follower(%d)\n", r.raftState.getCurrentTerm(), reply.Term)
				r.setState(Follower)
				r.setCurrentTerm(uint64(reply.Term))
				return
			}

			fid := wrapper.followerId
			r.indexLock.Lock()
			if reply.Success {
				// Update nextIndex and matchIndex.
				r.matchIndex[fid] = r.nextIndex[fid] - 1 // prevLogIndex
				r.nextIndex[fid] = r.getLastIndex() + 1
			} else {
				// Decrement nextIndex.
				r.nextIndex[fid]--
			}
			r.indexLock.Unlock()
		case <-lc.heartbeatTimerCh:
			// r.sendHeartbeatToAll(lc)
		case <-lc.appendEntriesTimerCh:
			r.sendAppendEntriesToAll(lc)
		case <-lc.updateCommitIndexTimerCh:
			r.updateLeaderCommit()
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case sc := <-r.startCh:
			newLog := &LogEntry{
				Index:   r.getLastIndex() + 1,
				Term:    r.getCurrentTerm(),
				Command: sc.command,
			}
			r.pushLogToLocal(newLog)
			r.setLastLog(newLog.Index, newLog.Term)
			r.matchIndex[r.me] = newLog.Index
			sc.respCh <- &StartResult{
				Index:    newLog.Index,
				Term:     newLog.Term,
				IsLeader: true,
			}
		}
	}
}

func (r *Raft) setState(state RaftState) {
	// r.Trace("state: %s -> %s\n", r.raftState.getState().String(), state.String())
	r.raftState.setState(state)
}

func (r *Raft) setCurrentTerm(term uint64) {
	// r.Trace("term: %d -> %d\n", r.raftState.getCurrentTerm(), term)
	r.raftState.setCurrentTerm(term)
}

// applyGoroutine is a running goroutine for apply logs.
func (r *Raft) applyGoroutine() {
	for !r.killed() {
		// Wait for notification.
		<-r.applyNotifyCh
		newCommitIndex := r.getCommitIndex()
		if newCommitIndex == r.getLastApplied() {
			continue
		}
		start := r.getLastApplied() + 1

		// Apply logs whose index is between start and newCommitIndex.
		r.logger.Debug("apply: [%d, %d]\n", start, newCommitIndex)
		applyMsgs := make([]ApplyMsg, newCommitIndex-start+1)
		for i := range applyMsgs {
			le := r.logEntries[i+int(start)]
			applyMsgs[i] = ApplyMsg{
				CommandValid:  true,
				Command:       le.Command,
				CommandIndex:  int(le.Index),
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: i,
			}
			r.applyCh <- applyMsgs[i]
		}
		r.setLastApplied(newCommitIndex)
	}
}

// debugPrintGoroutine is a running goroutine for printing debugging information in debug mode.
func (r *Raft) debugPrintGoroutine(interval time.Duration) {
	if r.logger.LogLevel() > LevelDebug {
		return
	}
	for !r.killed() {
		// Print something.
		time.Sleep(interval)
	}
}

// Make returns a raft server.
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
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me

	// Your initialization code here (2A, 2B, 2C).
	// States
	r.raftState = raftState{
		currentTerm:       1,
		commitIndex:       0,
		lastApplied:       0,
		lastSnapshotIndex: 0,
		lastSnapshotTerm:  0,
		lastLogIndex:      0,
		lastLogTerm:       0,
		state:             Follower,
		lastVoteFor:       -1,
		lastVoteTerm:      0,
		nextIndex:         make([]uint64, len(peers)),
		matchIndex:        make([]uint64, len(peers)),
	}
	// We need a dummy entry for convenience.
	r.logEntries = []*LogEntry{{
		Index: 0,
		Term:  0,
	}}
	r.leaderId = -1

	// Logger
	r.logger = newRaftLogger(r)

	r.SetLastContact()

	// Channels
	r.rpcCh = make(chan *RPC)
	r.killCh = make(chan struct{})
	r.applyCh = applyCh
	r.startCh = make(chan *StartCall)
	r.applyNotifyCh = make(chan bool)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// I don't want to implement it this way!
	// go rf.ticker()
	go r.run()
	go r.applyGoroutine()
	go r.debugPrintGoroutine(time.Millisecond * 1000)

	return r
}
