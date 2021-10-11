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
	if err != ErrorLogNotExist {
		t.Fatal(err.Error())
	}
}

func Test_logEntriesManager_FindFirstIndexByTerm(t *testing.T) {
	// [0 1 1 1 2 2 3 3 3]
	r := new(Raft)
	r.initializeDefault()
	r.persister = MakePersister()
	lem := NewLogEntriesManager(r)
	lem.PushLocal(&LogEntry{
		Index:   1,
		Term:    1,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   2,
		Term:    1,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   3,
		Term:    1,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   4,
		Term:    2,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   5,
		Term:    2,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   6,
		Term:    3,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   7,
		Term:    3,
		Command: nil,
	})
	lem.PushLocal(&LogEntry{
		Index:   8,
		Term:    3,
		Command: nil,
	})
	var firstIndex uint64
	var err error

	_, err = lem.FindFirstIndexByTerm(0)
	if err == nil {
		t.Fatal()
	}

	firstIndex, err = lem.FindFirstIndexByTerm(1)
	if firstIndex != 1 || err != nil {
		t.Fatal()
	}

	firstIndex, err = lem.FindFirstIndexByTerm(2)
	if firstIndex != 4 || err != nil {
		t.Fatal()
	}

	firstIndex, err = lem.FindFirstIndexByTerm(3)
	if firstIndex != 6 || err != nil {
		t.Fatal()
	}

	_, err = lem.FindFirstIndexByTerm(4)
	if err == nil {
		t.Fatal()
	}
}
