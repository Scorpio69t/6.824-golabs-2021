package raft

import (
	"testing"
)

func Test_logEntriesManager_Get(t *testing.T) {
	lem := logEntriesManager{}
	lem.logs = []*LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
		{
			Index:   1,
			Term:    1,
			Command: nil,
		},
		{
			Index:   2,
			Term:    2,
			Command: nil,
		},
		{
			Index:   3,
			Term:    3,
			Command: nil,
		},
	}
	lem.raftState = &raftState{
		lastLogIndex: 3,
		lastLogTerm:  3,
	}

	for i := 0; i <= 3; i++ {
		res, err := lem.Get(uint64(i))
		if err != nil {
			t.Fatal(err.Error())
		}
		if res.Term != uint64(i) {
			t.Fatal("Wrong log")
		}
	}

	_, err := lem.Get(4)
	if err != ErrorOutOfRange {
		t.Fatal(err.Error())
	}
}
