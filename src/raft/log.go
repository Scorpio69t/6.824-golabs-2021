package raft

import "sync/atomic"

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command interface{}
}

func (l *LogEntry) GetIndex() uint64 {
	return atomic.LoadUint64(&l.Index)
}

func (l *LogEntry) GetTerm() uint64 {
	return atomic.LoadUint64(&l.Term)
}

func (l *LogEntry) GetCommand() interface{} {
	return l.Command
}

func (l *LogEntry) SetIndex(val uint64) {
	atomic.StoreUint64(&l.Index, val)
}

func (l *LogEntry) SetTerm(val uint64) {
	atomic.StoreUint64(&l.Term, val)
}

type LogFuture struct {
	log    *LogEntry
	respCh chan bool
}
