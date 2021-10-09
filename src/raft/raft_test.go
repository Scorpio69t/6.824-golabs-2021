package raft

import (
	"testing"
)

func TestAppendEntriesConflictAfterPrevLog(t *testing.T) {
	// r:
	// [2  4  4]
	// entries:
	// [  (4) 7  7]
	// index:
	// [1  2  3  4]
	// expect to delete logs where index >= 3
	// final logs:
	// [2  4  7  7]
	applyCh := make(chan ApplyMsg, 10)
	r := &Raft{
		me:        0,
		leaderId:  1,
		persister: &Persister{},
		raftState: raftState{
			currentTerm: 7,
			commitIndex: 2,
			state:       Follower,
		},
		applyCh: applyCh,
	}
	r.logger = newRaftLogger(r)
	r.logEntriesManager = NewLogEntriesManager(r)
	r.logEntriesManager.logs = []*LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
		{
			Index:   1,
			Term:    2,
			Command: 101,
		},
		{
			Index:   2,
			Term:    4,
			Command: 102,
		},
		{
			Index:   3,
			Term:    4,
			Command: 103,
		},
	}
	args := &AppendEntriesArgs{
		Term:         7,
		LeaderId:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  4,
		Entries: []*LogEntry{
			{
				Index:   3,
				Term:    7,
				Command: 104,
			},
			{
				Index:   4,
				Term:    7,
				Command: 105,
			},
		},
		LeaderCommit: 4,
	}

	rpc := &RPC{
		Args:   args,
		respCh: make(chan RPCResponse, 1),
	}

	r.appendEntries(rpc, args)
	resp := <-rpc.respCh
	reply := resp.Response.(*AppendEntriesReply)
	if !reply.Success {
		t.Fatalf("reply false, expected true.\n")
	}
	if reply.Term != 7 {
		t.Fatalf("wrong reply.Term")
	}

	expectedApplyMsg := []ApplyMsg{
		{
			CommandValid: false,
			CommandIndex: 3,
			Command:      103,
		},
		{
			CommandValid: true,
			CommandIndex: 3,
			Command:      104,
		},
		{
			CommandValid: true,
			CommandIndex: 4,
			Command:      105,
		},
	}
	for _, expect := range expectedApplyMsg {
		msg := <-applyCh
		if msg.CommandIndex != expect.CommandIndex || msg.Command.(int) != expect.Command.(int) || msg.CommandValid != expect.CommandValid {
			t.Fatalf("wrong apply msg. expected=%v, but %v\n", expect, msg)
		}
	}
}

func TestAppendEntriesConflictAtPrevLog(t *testing.T) {
	// r:
	// [2 4  4]
	// entries:
	// [ (5) 7 7]
	// index:
	// [1 2  3 4]
	// expect to delete logs where index >= 3
	// final logs:
	// [2 5 7 7]
	applyCh := make(chan ApplyMsg, 10)
	r := &Raft{
		me:        0,
		leaderId:  1,
		persister: &Persister{},
		raftState: raftState{
			currentTerm: 7,
			commitIndex: 1,
			state:       Follower,
		},
		applyCh: applyCh,
	}
	r.logger = newRaftLogger(r)
	r.logEntriesManager = NewLogEntriesManager(r)
	r.logEntriesManager.logs = []*LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
		{
			Index:   1,
			Term:    2,
			Command: 101,
		},
		{
			Index:   2,
			Term:    4,
			Command: 102,
		},
		{
			Index:   3,
			Term:    4,
			Command: 103,
		},
	}

	args := &AppendEntriesArgs{
		Term:         7,
		LeaderId:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  5,
		Entries: []*LogEntry{
			{
				Index:   3,
				Term:    7,
				Command: 104,
			},
			{
				Index:   4,
				Term:    7,
				Command: 105,
			},
		},
		LeaderCommit: 4,
	}

	rpc := &RPC{
		Args:   args,
		respCh: make(chan RPCResponse, 1),
	}

	r.appendEntries(rpc, args)
	resp := <-rpc.respCh
	reply := resp.Response.(*AppendEntriesReply)
	if reply.Success {
		t.Fatalf("reply true, expected false.\n")
	}
	if reply.Term != 7 {
		t.Fatalf("wrong reply.Term")
	}

	expectedApplyMsg := []ApplyMsg{
		{
			CommandValid: false,
			CommandIndex: 2,
			Command:      102,
		},
		{
			CommandValid: false,
			CommandIndex: 3,
			Command:      103,
		},
	}
	for _, expect := range expectedApplyMsg {
		msg := <-applyCh
		if msg.CommandIndex != expect.CommandIndex || msg.Command.(int) != expect.Command.(int) || msg.CommandValid != expect.CommandValid {
			t.Fatalf("wrong apply msg. expected=%v, but %v\n", expect, msg)
		}
	}
}

func TestAppendEntriesNoConflict(t *testing.T) {
	// r:
	// [2 4]
	// entries:
	// [ (4) 7 7]
	// index:
	// [1 2  3 4]
	// expect to delete logs where index >= 3
	// final logs:
	// [2 5 7 7]
	applyCh := make(chan ApplyMsg, 10)
	r := &Raft{
		me:        0,
		leaderId:  1,
		persister: &Persister{},
		raftState: raftState{
			currentTerm: 6,
			commitIndex: 1,
			state:       Follower,
		},
		applyCh: applyCh,
	}
	r.logger = newRaftLogger(r)
	r.logEntriesManager = NewLogEntriesManager(r)
	r.logEntriesManager.logs = []*LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
		{
			Index:   1,
			Term:    2,
			Command: 101,
		},
		{
			Index:   2,
			Term:    4,
			Command: 102,
		},
	}
	args := &AppendEntriesArgs{
		Term:         6,
		LeaderId:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  4,
		Entries: []*LogEntry{
			{
				Index:   3,
				Term:    6,
				Command: 104,
			},
			{
				Index:   4,
				Term:    6,
				Command: 105,
			},
		},
		LeaderCommit: 4,
	}

	rpc := &RPC{
		Args:   args,
		respCh: make(chan RPCResponse, 1),
	}

	r.appendEntries(rpc, args)
	resp := <-rpc.respCh
	reply := resp.Response.(*AppendEntriesReply)
	if !reply.Success {
		t.Fatalf("reply false, expected true.\n")
	}
	if reply.Term != 6 {
		t.Fatalf("wrong reply.Term")
	}

	expectedApplyMsg := []ApplyMsg{
		{
			CommandValid: true,
			CommandIndex: 2,
			Command:      102,
		},
		{
			CommandValid: true,
			CommandIndex: 3,
			Command:      104,
		},
		{
			CommandValid: true,
			CommandIndex: 4,
			Command:      105,
		},
	}
	for _, expect := range expectedApplyMsg {
		msg := <-applyCh
		if msg.CommandIndex != expect.CommandIndex || msg.Command.(int) != expect.Command.(int) || msg.CommandValid != expect.CommandValid {
			t.Fatalf("wrong apply msg. expected=%v, but %v\n", expect, msg)
		}
	}
}

func TestRaft_persist(t *testing.T) {
	r := new(Raft)
	lastIndex := uint64(3)
	lastTerm := uint64(3)
	currentTerm := uint64(3)
	lastVoteFor := int32(2)
	lastVoteTerm := uint64(2)
	logs := []*LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
		{
			Index:   1,
			Term:    1,
			Command: 100,
		},
		{
			Index:   2,
			Term:    2,
			Command: 101,
		},
		{
			Index:   3,
			Term:    3,
			Command: 102,
		},
	}
	r.persister = MakePersister()
	r.initializeDefault()
	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastIndex, lastTerm)
	r.SetLastVoteFor(lastVoteFor)
	r.SetLastVoteTerm(lastVoteTerm)
	r.logEntriesManager.SetLogs(logs)
	r.persist()

	r.initializeDefault()
	r.readPersist(r.persister.ReadRaftState())
	if r.getCurrentTerm() != currentTerm || r.LastVoteFor() != lastVoteFor || r.LastVoteTerm() != lastVoteTerm {
		t.Fatalf("wrong persistence state")
	}
	if len(r.logEntriesManager.logs) != len(logs) {
		t.Fatalf("wrong logs length")
	}
	for i := range logs {
		if *logs[i] != *r.logEntriesManager.logs[i] {
			t.Fatalf("wrong log command")
		}
	}
}

func TestRaft_persist_without_state(t *testing.T) {
	r := new(Raft)
	r.persister = MakePersister()
	r.initializeDefault()

	// We had not persisted the raft state before,
	// so the state will be expected not to change.
	r.readPersist(r.persister.ReadRaftState())
	if r.getCurrentTerm() != 1 || r.LastVoteFor() != -1 || r.LastVoteTerm() != 0 {
		t.Fatalf("wrong persistence state")
	}
	if len(r.logEntriesManager.logs) != 1 {
		t.Fatalf("wrong logs length")
	}
}
