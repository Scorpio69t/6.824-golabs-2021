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
		logEntries: []*LogEntry{
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
		},
	}
	r.logger = newRaftLogger(r)
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
	r.updateLastLog()

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
		logEntries: []*LogEntry{
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
		},
	}
	r.logger = newRaftLogger(r)
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
	r.updateLastLog()

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
		logEntries: []*LogEntry{
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
		},
	}
	r.logger = newRaftLogger(r)
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
	r.updateLastLog()

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
