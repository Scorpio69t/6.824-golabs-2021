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
	logger log.Logger
	debug  bool

	// Your data here (2A, 2B, 2C).
	leaderId         int
	state            *raftState
	lastContact      time.Time
	lastContactMutex sync.RWMutex

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Channels for communication
	rpcCh  chan RPC
	killCh chan struct{}
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
	return int(rf.state.getCurrentTerm()), rf.state.getState() == Leader
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
	rf.Debug("RPC: receive RequestVote(%s): candidate=%d, term=%d\n", session, args.CandidateId, args.Term)
	defer func() {
		rf.Debug("RPC: answer RequestVote(%s): candidate=%d, term=%d, granted=%d\n", session, args.CandidateId, reply.Term, reply.VoteGranted)
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
		r.Warn("Leader existed. leaderId=%d, candidateId=%d", r.leaderId, args.CandidateId)
		return
	}

	// Ignore older term.
	if args.Term < int(r.state.getCurrentTerm()) {
		return
	}

	// Found newer term, transfer to follower and update term.
	if args.Term > int(r.state.getCurrentTerm()) {
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
	}

	// Have voted in this term.
	if r.state.LastVoteTerm() == uint64(args.Term) && r.state.LastVoteFor() != -1 {
		r.Warn("Duplicated RequestVote, term=%d\n", args.Term)
		if r.state.LastVoteFor() == int32(args.CandidateId) {
			resp.VoteGranted = true
		}
		return
	}

	// Compare lastTerm and lastIndex
	lastIndex, lastTerm := r.state.getLastLog()
	if lastTerm > uint64(args.Term) {
		r.Info("Our lastTerm is greater.\n")
		return
	}
	if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		r.Info("Our term is equal to args's, but the lastIndex is greater.\n")
		return
	}

	// Grant vote for the candidate.
	r.Info("Vote for candidate=%d, term=%d\n", args.CandidateId, args.Term)
	resp.VoteGranted = true
	resp.Term = args.Term
	r.state.SetLastVoteTerm(r.state.getCurrentTerm())
	r.state.SetLastVoteFor(int32(args.CandidateId))
	r.SetLastContact()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*Log
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	session := uuid.NewString()
	rf.Debug("RPC: receive AppendEntries(%s): leader=%d, term=%d\n", session, args.LeaderId, args.Term)
	defer func() {
		rf.Debug("RPC: answer AppendEntries(%s): term=%d, success=%d", session, reply.Term, reply.Success)
	}()

	response := rf.handleRPC(args)
	reply.Success = response.Response.(*AppendEntriesReply).Success
	reply.Term = response.Response.(*AppendEntriesReply).Term
}

func (r *Raft) appendEntries(rpc RPC, args *AppendEntriesArgs) {
	// TODO: 2B
	// Defer return rpc.
	resp := &AppendEntriesReply{
		Term:    int(r.state.getCurrentTerm()),
		Success: false,
	}
	var err error
	defer func() {
		rpc.Response(resp, err)
	}()

	if r.state.getCurrentTerm() > uint64(args.Term) {
		r.Debug("RPC: appendEntries: receive older term\n")
		return
	}

	if r.me != args.LeaderId && (r.state.getCurrentTerm() < uint64(args.Term) || r.state.getState() != Follower) {
		r.Debug("RPC: appendEntries: find new leader: leader=%d, term=%d\n", args.LeaderId, args.Term)
		r.setState(Follower)
		r.setCurrentTerm(uint64(args.Term))
		resp.Term = args.Term
	}

	// Success
	r.Debug("RPC: appendEntries: success\n")
	r.SetLastContact()
	r.leaderId = args.LeaderId
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	r.Debug("run")
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
		switch r.state.getState() {
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
	// TODO: 2A
	r.Debug("runFollower")
	heartbeatTimer := randomTimeout(time.Millisecond * 150)

	// A long loop for follower.
	for r.state.getState() == Follower {
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
			r.setCurrentTerm(r.state.getCurrentTerm() + 1)
			r.setState(Candidate)
			return
		}
	}

}

func (r *Raft) runCandidate() {
	r.Debug("runCandidate")
	r.Info("start election: term=%d\n", r.state.getCurrentTerm())
	electionTimeout := randomTimeout(time.Millisecond * 2000)

	// Vote channel.
	voteCh := make(chan *RequestVoteReply, 5)
	done := false
	doneMutex := sync.Mutex{}
	defer func() {
		doneMutex.Lock()
		defer func() {
			doneMutex.Unlock()
			r.Debug("leave runCandidate")
		}()

		close(voteCh)
		done = true
	}()

	// Votes counter.
	grantedVotes := 0
	neededVotes := (len(r.peers) + 1) / 2

	// Vote for self.
	voteCh <- &RequestVoteReply{
		Term:        int(r.state.currentTerm),
		VoteGranted: true,
	}
	r.state.SetLastVoteFor(int32(r.me))
	r.state.SetLastVoteTerm(r.state.currentTerm)

	// Send RequestVote RPC
	args := &RequestVoteArgs{
		Term:         int(r.state.getCurrentTerm()),
		CandidateId:  r.me,
		LastLogIndex: r.state.getLastIndex(),
		LastLogTerm:  r.state.lastLogTerm,
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

	// A long loop for waiting votes.
	for r.state.getState() == Candidate {
		select {
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		case <-electionTimeout:
			// Increase term and start a new round of election.
			r.Info("electionTimeout\n")
			r.setCurrentTerm(r.state.getCurrentTerm() + 1)
			return
		case reply := <-voteCh:
			// Find higher term, become the follower state.
			if reply.Term > int(r.state.currentTerm) {
				r.setCurrentTerm(uint64(reply.Term))
				r.setState(Follower)
				return
			}

			// Receive grantedVote.
			if reply.VoteGranted {
				grantedVotes++
				if grantedVotes >= neededVotes {
					// Switch to the leader state.
					r.Info("election won, term=%d\n", r.state.getCurrentTerm())
					r.leaderId = r.me
					r.setState(Leader)
					return
				}
			}
		}
	}
}

func (r *Raft) runLeader() {
	r.Debug("runLeader")

	// transferToFollowerCh will not be closed until stillLeader becomes false
	stillLeader := true
	stillLeaderMutex := sync.Mutex{}
	transferToFollowerCh := make(chan int, 1)

	// Close the channel.
	defer func() {
		stillLeaderMutex.Lock()
		defer stillLeaderMutex.Unlock()

		close(transferToFollowerCh)
		stillLeader = false
	}()

	// Send heartbeats.
	go func() {
		for {
			stillLeaderMutex.Lock()
			if !stillLeader {
				stillLeaderMutex.Unlock()
				return
			}
			stillLeaderMutex.Unlock()
			
			args := &AppendEntriesArgs{
				Term:     int(r.state.getCurrentTerm()),
				LeaderId: r.me,
				// TODO: 2B
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			for peer := range r.peers {
				if peer == r.me {
					continue
				}
				go func(server int) {
					reply := &AppendEntriesReply{}
					ok := r.sendAppendEntries(server, args, reply)

					stillLeaderMutex.Lock()
					defer stillLeaderMutex.Unlock()
					if stillLeader && ok && reply.Term > int(r.state.getCurrentTerm()) {
						transferToFollowerCh <- reply.Term
					}
				}(peer)
			}

			time.Sleep(150 * time.Millisecond)
		}
	}()

	for r.state.getState() == Leader {
		select {
		case term := <-transferToFollowerCh:
			r.Info("find higher term, leader(%d) -> follower(%d\n", r.state.getCurrentTerm(), term)
			r.setState(Follower)
			r.setCurrentTerm(uint64(term))
			return
		case <-r.killCh:
			return
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		}
	}
}

func (r *Raft) setState(state RaftState) {
	if r.debug {
		r.Debug("state: %s -> %s\n", r.state.getState().String(), state.String())
	}
	r.state.setState(state)
}

func (r *Raft) setCurrentTerm(term uint64) {
	if r.debug {
		r.Debug("term: %d -> %d\n", r.state.getCurrentTerm(), term)
	}
	r.state.setCurrentTerm(term)
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
	r.state = &raftState{}
	r.setCurrentTerm(0)
	r.state.setCommitIndex(0)
	r.state.setLastApplied(0)
	r.state.setLastSnapshot(0, 0)
	r.state.setLastLog(0, 0)
	r.setState(Follower)
	r.state.SetLastVoteFor(-1)
	r.state.SetLastVoteTerm(0)
	r.leaderId = -1

	// Debug
	r.logger = *log.Default()
	r.logger.SetFlags(log.Lmicroseconds)
	r.debug = true

	r.SetLastContact()

	// Channels
	r.rpcCh = make(chan RPC, 3)
	r.killCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// I don't want to implement it this way!
	// go rf.ticker()
	go r.run()

	// Debug
	go func() {
		for !r.killed() {
			// r.Debug("lastVoteTerm=%d, voteFor=%d", r.state.LastVoteTerm(), r.state.LastVoteFor())
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	return r
}
