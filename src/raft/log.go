package raft

import "sync/atomic"

type LogEntry struct {
	index   uint64
	term    uint64
	command interface{}
}

func (l *LogEntry) Index() uint64 {
	return atomic.LoadUint64(&l.index)
}

func (l *LogEntry) Term() uint64 {
	return atomic.LoadUint64(&l.term)
}

func (l *LogEntry) Command() interface{} {
	return l.command
}

func (l *LogEntry) SetIndex(val uint64) {
	atomic.StoreUint64(&l.index, val)
}

func (l *LogEntry) SetTerm(val uint64) {
	atomic.StoreUint64(&l.term, val)
}

type LogFuture struct {
	log    *LogEntry
	respCh chan bool
}
