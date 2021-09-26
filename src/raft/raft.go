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
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"github.com/google/uuid"
)

//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Debug
	logger   log.Logger
	logLevel int

	// Your data here (2A, 2B, 2C).
	leaderId int
	raftState
	lastContact      time.Time
	lastContactMutex sync.RWMutex
	logEntries       []*LogEntry

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Channels for communication
	rpcCh   chan RPC
	killCh  chan struct{}
	applyCh chan ApplyMsg
	logCh   chan *LogFuture

	// Mutex
	// Protect r.Start().
	startLock sync.Mutex
}

// pushLogToLocal append a new entry to local logEntries.
// The function is not thread-safe, and it is necessary to accquire lastLock before being called.
func (r *Raft) pushLogToLocal(log *LogEntry) {
	r.Info("pushToLocal: oldIndex=%d, oldTerm=%d, newIndex=%d, newTerm=%d\n", r.lastLogIndex, r.lastLogTerm, log.Index, log.Term)
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.raftState.getCurrentTerm()), rf.raftState.getState() == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r *Raft) handleRPC(args interface{}) (response RPCResponse) {
	// r.Debug("RPC: enter handleRPC\n")
	// defer r.Debug("RPC: leave handleRPC\n")

	respCh := make(chan RPCResponse)
	rpc := MakeRPC(args, respCh)
	r.rpcCh <- rpc

	// r.Debug("RPC: wait for respCh\n")
	resp := <-respCh
	return resp
}

func (r *Raft) processRPC(rpc RPC) {
	switch args := rpc.Args.(type) {
	case *RequestVoteArgs:
		r.requestVote(rpc, args)
	case *AppendEntriesArgs:
		r.appendEntries(rpc, args)
	default:
		r.Error("Unknown RPC type.\n")
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	session := uuid.NewString()
	rf.Debug("RPC: RequestVote(%s): called: candidate=%d, term=%d\n", session, args.CandidateId, args.Term)
	defer func() {
		rf.Debug("RPC: RequestVote(%s): returned: candidate=%d, term=%d, granted=%d\n", session, args.CandidateId, reply.Term, reply.VoteGranted)
	}()

	response := rf.handleRPC(args)
	reply.Term = response.Response.(*RequestVoteReply).Term
	reply.VoteGranted = response.Response.(*RequestVoteReply).VoteGranted
}

func (r *Raft) requestVote(rpc RPC, args *RequestVoteArgs) {
	// r.Debug("RPC: enter requestVote\n")
	// defer r.Debug("RPC: leave requestVote\n")

	resp := &RequestVoteReply{}
	var err error
	defer func() {
		rpc.Response(resp, err)
	}()

	// Leader already existed.
	if r.leaderId != -1 && r.leaderId != args.CandidateId {
		r.Warn("RPC requestVote: Leader existed. leaderId=%d, candidateId=%d", r.leaderId, args.CandidateId)
		return
	}

	// Ignore older term.
	if args.Term < int(r.raftState.getCurrentTerm()) {
		return
	}

	// Discover newer term, convert to follower and update term.
	if args.Term > int(r.raftState.getCurrentTerm()) {
		r.Info("RPC requestVote: discover newer term.")
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
	}

	// Have voted in this term.
	if r.raftState.LastVoteTerm() == uint64(args.Term) && r.raftState.LastVoteFor() != -1 {
		r.Warn("RPC requestVote: duplicated RequestVote term, term=%d\n", args.Term)
		if r.raftState.LastVoteFor() == int32(args.CandidateId) {
			resp.VoteGranted = true
		}
		return
	}

	// Compare lastTerm and lastIndex
	lastIndex, lastTerm := r.raftState.getLastLog()
	if lastTerm > uint64(args.Term) {
		r.Info("RPC requestVote: our lastTerm is greater.\n")
		return
	}
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		r.Info("RPC requestVote: our term is equal to args's, but the lastIndex is greater.\n")
		return
	}

	// Grant vote for the candidate.
	r.Info("RPC requestVote: vote for candidate=%d, term=%d\n", args.CandidateId, args.Term)
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
	FollowerId int
	Term       int
	Success    bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	session := uuid.NewString()
	var t string
	if args.Entries == nil && args.LeaderCommit == 0 {
		t = "Heartbeat"
		rf.Trace("RPC: %s(%s): called: leader=%d, term=%d, prevLogIndex=%d, leaderCommit=%d\n",
			t, session, args.LeaderId, args.Term, args.PrevLogIndex, args.LeaderCommit)
		defer func() {
			rf.Trace("RPC: %s(%s): returned: term=%d, success=%d", t, session, reply.Term, reply.Success)
		}()
	} else {
		t = "AppendEntries"
		rf.Debug("RPC: %s(%s): called: leader=%d, term=%d, prevLogIndex=%d, leaderCommit=%d\n",
			t, session, args.LeaderId, args.Term, args.PrevLogIndex, args.LeaderCommit)
		defer func() {
			rf.Debug("RPC: %s(%s): returned: term=%d, success=%d", t, session, reply.Term, reply.Success)
		}()
	}

	response := rf.handleRPC(args)
	reply.Success = response.Response.(*AppendEntriesReply).Success
	reply.Term = response.Response.(*AppendEntriesReply).Term
}

func (r *Raft) appendEntries(rpc RPC, args *AppendEntriesArgs) {
	// Defer return rpc.
	resp := &AppendEntriesReply{
		Term:    int(r.raftState.getCurrentTerm()),
		Success: false,
	}
	var err error
	defer func() {
		rpc.Response(resp, err)
	}()

	updateLastLog := func() {
		newLastLog := r.logEntries[len(r.logEntries)-1]
		r.lastLogIndex = newLastLog.Index
		r.lastLogTerm = newLastLog.Term
		r.Info("RPC: appendEntries: updateLastLog: index=%d, term=%d\n", newLastLog.Index, newLastLog.Term)
	}

	// Discover older leader, return directly.
	if r.raftState.getCurrentTerm() > uint64(args.Term) {
		r.Warn("RPC: appendEntries: receive older term\n")
		// Reply false.
		return
	}

	// Discover newer leader, convert to follower.
	if r.me != args.LeaderId && (r.raftState.getCurrentTerm() < uint64(args.Term) || r.raftState.getState() != Follower) {
		r.Info("RPC: appendEntries: find new leader: leader=%d, term=%d\n", args.LeaderId, args.Term)
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
		resp.Term = args.Term
	}

	// This RPC is valid, update r.lastContact and r.leaderId.
	r.SetLastContact()
	r.leaderId = args.LeaderId

	// Heartbeat
	if args.Entries == nil && args.LeaderCommit == 0 {
		resp.Success = true
		return
	}

	// Actual AppendEntries RPC
	r.lastLock.Lock()
	defer r.lastLock.Unlock()

	// Doesn’t contain an entry at prevLogIndex who·se term matches prevLogTerm.
	if r.lastLogIndex < args.PrevLogIndex {
		r.Info("RPC: appendEntries: don't contain an entry at prevLogIndex: prevLogIndex=%d\n", args.PrevLogIndex)
		// Reply false.
		return
	}
	if r.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Delete conflict entries.
		r.Info("RPC: appendEntries: prevLogIndex conflict: index=%d, term=%d, expectedTerm=%d\n",
			args.PrevLogIndex, r.logEntries[args.PrevLogIndex].Term, args.PrevLogTerm)
		r.logEntries = r.logEntries[:args.PrevLogIndex]
		updateLastLog()
		// Reply false.
		return
	}

	// Append any new entries not already in the log.
	newEntries := []*LogEntry{}
	for i, newEntry := range args.Entries {
		if r.lastLogIndex >= newEntry.Index {
			if r.logEntries[r.lastLogIndex].Term != newEntry.Term {
				// Find conflict logs. Delete conflict logs and do not append any entries.
				r.logEntries = r.logEntries[:newEntry.Index]
				updateLastLog()

				// Reply true.
				resp.Success = true
				return
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
	updateLastLog()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	oldCommitIndex := r.getCommitIndex()
	if oldCommitIndex < args.LeaderCommit {
		r.Info("RPC: appendEntries: attempt to update commitIndex\n")
		var newCommitIndex uint64
		if args.LeaderCommit < r.lastLogIndex {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = r.lastLogIndex
		}
		// Apply log entries in [oldCommitIndex, newCommitIndex]
		r.indexLock.Lock()
		for i := oldCommitIndex; i <= uint64(newCommitIndex); i++ {
			r.apply(int(i))
		}
		r.indexLock.Unlock()
	}
	resp.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.Debug("RPC: sendRequestVote, server=%d\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.Warn("RPC: sendRequestVote failed.\n")
	}
	return ok
}

//
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
	r.startLock.Lock()
	defer r.startLock.Unlock()
	defer func() {
		r.Debug("Start: returned. command=%v, index=%d, term=%d, isLeader=%d\n", command, index, term, isLeader)
	}()

	index = int(r.getLastIndex()) + 1
	term = int(r.raftState.getCurrentTerm())
	isLeader = r.raftState.getState() == Leader

	// Do nothing if it is not the leader.
	if !isLeader {
		return
	}

	// Send new entry to r.logCh
	logFuture := &LogFuture{
		log: &LogEntry{
			Term:    uint64(term),
			Index:   uint64(index),
			Command: command,
		},
		respCh: make(chan bool),
	}
	r.logCh <- logFuture

	// Wait for completing appending the log. The step is for thread-safety.
	success := <-logFuture.respCh
	if !success {
		r.Warn("Start: failed.")
	}

	return
}

//
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.setState(Shutdown)
	rf.killCh <- struct{}{}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// run is a long running goroutine.
func (r *Raft) run() {
	for !r.killed() {
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
			r.Debug("run(): unknown state, return.\n")
			return
		}
	}
}

func (r *Raft) runFollower() {
	r.Info("runFollower: called\n")
	heartbeatTimer := randomTimeout(time.Millisecond * 150)

	// A long loop for follower.
	for r.raftState.getState() == Follower {
		select {
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case <-heartbeatTimer:
			// Clear the timeout.
			heartbeatTimeout := randomTimeoutInt((int(time.Millisecond) * 150))
			heartbeatTimer = randomTimeout(time.Duration(heartbeatTimeout))

			// Success.
			if time.Now().Sub(r.LastContact()) < time.Millisecond*250 {
				continue
			}

			// Switch to the candidate state.
			r.leaderId = -1
			r.setCurrentTerm(r.raftState.getCurrentTerm() + 1)
			r.setState(Candidate)
			return
		case lf := <-r.logCh:
			r.Warn("receive from logCh, but not leader.")
			lf.respCh <- false
		}
	}

}

func (r *Raft) setupCandidate() (voteCh chan *RequestVoteReply, setDone func()) {
	// Vote channel.
	voteCh = make(chan *RequestVoteReply, 5)
	done := false
	doneMutex := sync.Mutex{}

	// Lambda function for finishing the round of election.
	setDone = func() {
		doneMutex.Lock()
		defer doneMutex.Unlock()
		done = true
		close(voteCh)
	}

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

			doneMutex.Lock()
			if !done && ok {
				voteCh <- reply
			}
			doneMutex.Unlock()
		}(peer)
	}

	return
}

func (r *Raft) runCandidate() {
	r.Info("runCandidate: start election: term=%d\n", r.raftState.getCurrentTerm())
	electionTimeout := randomTimeout(time.Millisecond * 2000)

	voteCh, setDone := r.setupCandidate()
	defer func() {
		r.Debug("runCandidate: returned\n")
		setDone()
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
			r.Info("electionTimeout\n")
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
					r.Info("election won, term=%d\n", r.raftState.getCurrentTerm())
					r.leaderId = r.me
					r.setState(Leader)
					return
				}
			}
		case lf := <-r.logCh:
			r.Warn("receive from logCh, but not leader.")
			lf.respCh <- false
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

	// Not necessary to replica log entries.
	if r.lastLogIndex < r.matchIndex[server] {
		err = ErrorFollowerUpToDate
		return
	}

	// Valid AppendEntries RPC.
	prevLog := r.logEntries[r.nextIndex[server]-1]
	args = &AppendEntriesArgs{
		Term:         int(r.getCurrentTerm()),
		LeaderId:     r.me,
		LeaderCommit: r.commitIndex,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      r.logEntries[r.nextIndex[server]:],
	}

	return
}

type appendEntriesReplyWrapper struct {
	followerId int
	reply      *AppendEntriesReply
}

func (r *Raft) setupLeader() (appendEntriesReplyCh chan *appendEntriesReplyWrapper, closeAppendEntriesReplyCh func()) {
	for i := range r.peers {
		r.nextIndex[i] = r.lastLogIndex + 1
		r.matchIndex[i] = 0
	}
	r.leaderId = r.me
	r.setCommitIndex(0)

	// transferToFollowerCh will not be closed until stillLeader becomes false.
	stillLeader := true
	stillLeaderMutex := sync.Mutex{}
	getStillLeader := func() bool {
		stillLeaderMutex.Lock()
		defer stillLeaderMutex.Unlock()
		return stillLeader
	}
	appendEntriesReplyCh = make(chan *appendEntriesReplyWrapper)

	// Lambda for closing the reply channel.
	closeAppendEntriesReplyCh = func() {
		stillLeaderMutex.Lock()
		defer stillLeaderMutex.Unlock()

		close(appendEntriesReplyCh)
		stillLeader = false
	}

	// Lambda for sending AppendEntries RPC periodically.
	sendAppendEntriesRPC := func(
		newArgs func(server int) (*AppendEntriesArgs, error),
		replyHandler func(reply *appendEntriesReplyWrapper),
	) {
		for getStillLeader() {
			for peer := range r.peers {
				// Skip me.
				if peer == r.me {
					continue
				}

				// Build args
				args, err := newArgs(peer)
				if err == ErrorSendToMe || err == ErrorFollowerUpToDate {
					r.Info("Follower %d: "+err.Error(), peer)
					continue
				} else if err != nil {
					r.Warn("Follower %d: "+err.Error(), peer)
				}

				// Run a goroutine to send RPC
				go func(server int) {
					reply := &AppendEntriesReply{}
					ok := r.sendAppendEntries(server, args, reply)

					stillLeaderMutex.Lock()
					defer stillLeaderMutex.Unlock()
					if stillLeader && ok {
						replyHandler(&appendEntriesReplyWrapper{
							followerId: server,
							reply:      reply,
						})
					}
				}(peer)
			}

			time.Sleep(150 * time.Millisecond)
		}
	}

	// Send heartbeats.
	go func() {
		sendAppendEntriesRPC(
			func(server int) (*AppendEntriesArgs, error) {
				// Build heartbeat args.
				return r.newAppendEntriesArgs(server, true)
			},
			func(reply *appendEntriesReplyWrapper) {
				// Send reply to appendEntriesReplyCh only if finding newer term.
				if reply.reply.Term > int(r.getCurrentTerm()) {
					appendEntriesReplyCh <- reply
				}
			})
	}()

	// Send AppendEntries RPC.
	go func() {
		sendAppendEntriesRPC(
			func(server int) (args *AppendEntriesArgs, err error) {
				// Build actual AppendEntries args.
				args, err = r.newAppendEntriesArgs(server, false)
				r.Trace("sendAppendEntries: to=%d, prevLogIndex=%d, leaderCommit=%d\n", server, args.PrevLogIndex, args.LeaderCommit)
				return
			},
			func(reply *appendEntriesReplyWrapper) {
				// Always send reply to appendEntriesReplyCh.
				appendEntriesReplyCh <- reply
			},
		)
	}()

	// Update commitIndex periodically.
	go func() {
		for getStillLeader() {
			// For each N, where N is in [commitIndex + 1, lastIndex], and log[N].Term == currentTerm,
			// we set commitIndex = N if a majority of matchIndex[i] >= N.
			// ? TODO: Should r.lastLock be accquired?
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
					r.Error("Goroutine::UpdateCommitIndex(): newer term is found.")
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
				r.Info("apply entries: [%d, %d]\n", r.commitIndex+1, newCommitIndex)
				for i := r.commitIndex + 1; i <= newCommitIndex; i++ {
					r.apply(int(i))
				}
				r.indexLock.Unlock()
			}

			time.Sleep(300 * time.Millisecond)
		}
	}()

	return
}

func (r *Raft) runLeader() {
	r.Info("runLeader: called\n")

	// Reinitialize and send AppendEntries RPC periodically.
	appendEntriesReplyCh, closeAppendEntriesReplyCh := r.setupLeader()

	defer func() {
		r.Info("runLeader: returned\n")
		closeAppendEntriesReplyCh()
	}()

	for r.raftState.getState() == Leader {
		select {
		case wrapper := <-appendEntriesReplyCh:
			reply := wrapper.reply
			// Discover newer term, convert to follower.
			if reply.Term > int(r.getCurrentTerm()) {
				r.Info("find higher term, leader(%d) -> follower(%d)\n", r.raftState.getCurrentTerm(), reply.Term)
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

		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case lf := <-r.logCh:
			l := lf.log
			// Append
			r.pushLogToLocal(l)
			r.setLastLog(l.Index, l.Term)
			r.indexLock.Lock()
			r.matchIndex[r.me] = l.Index
			r.indexLock.Unlock()
			lf.respCh <- true
		}
	}
}

func (r *Raft) setState(state RaftState) {
	r.Trace("state: %s -> %s\n", r.raftState.getState().String(), state.String())
	r.raftState.setState(state)
}

func (r *Raft) setCurrentTerm(term uint64) {
	r.Trace("term: %d -> %d\n", r.raftState.getCurrentTerm(), term)
	r.raftState.setCurrentTerm(term)
}

// apply send ApplyMsg to applyCh.
// The function is not thread-safe. Need to accquire indexLock before calling it.
func (r *Raft) apply(index int) {
	// Send message to applyCh.
	msg := ApplyMsg{
		CommandValid:  true,
		Command:       r.logEntries[index].Command,
		CommandIndex:  int(r.logEntries[index].Index),
		SnapshotValid: false,
		Snapshot:      []byte{},
		SnapshotTerm:  0,
		SnapshotIndex: index,
	}
	r.applyCh <- msg
	r.Info("apply: index=%d, command=%d\n", msg.CommandIndex, msg.Command)

	// Update commitIndex.
	if r.commitIndex < uint64(index) {
		r.commitIndex = uint64(index)
		r.Info("apply: set commitIndex = %d\n", index)
	}
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

	// Debug
	r.logger = *log.Default()
	r.logger.SetFlags(log.Lmicroseconds)
	r.logLevel = DefaultLogLevel

	r.SetLastContact()

	// Channels
	r.rpcCh = make(chan RPC, 3)
	r.killCh = make(chan struct{}, 1)
	r.applyCh = applyCh
	r.logCh = make(chan *LogFuture)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// I don't want to implement it this way!
	// go rf.ticker()
	go r.run()

	// Debug
	go func() {
		for !r.killed() {
			// r.Trace("lastVoteTerm=%d, voteFor=%d, commitIndex=%d\n", r.LastVoteTerm(), r.LastVoteFor(), r.getCommitIndex())
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	return r
}
