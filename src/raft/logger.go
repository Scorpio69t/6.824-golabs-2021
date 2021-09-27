package raft

import (
	"fmt"
)

const (
	LevelTrace = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
)

func (r *Raft) Warn(format string, args ...interface{}) {
	if r.logLevel > LevelWarn {
		return
	}
	format = fmt.Sprintf("WARN  %s", format)
	r.Print(format, args...)
}

func (r *Raft) Debug(format string, args ...interface{}) {
	if r.logLevel > LevelDebug {
		return
	}
	format = fmt.Sprintf("DEBUG %s", format)
	r.Print(format, args...)
}

func (r *Raft) Info(format string, args ...interface{}) {
	if r.logLevel > LevelInfo {
		return
	}
	format = fmt.Sprintf("INFO  %s", format)
	r.Print(format, args...)
}

func (r *Raft) Error(format string, args ...interface{}) {
	if r.logLevel > LevelError {
		return
	}
	format = fmt.Sprintf("ERROR %s", format)
	r.Print(format, args...)
}

func (r *Raft) Print(format string, args ...interface{}) {
	format = fmt.Sprintf("{id=%d, state=%s, term=%d}: %s", r.me, formatLength(r.raftState.getState().String(), 9), r.raftState.getCurrentTerm(), format)
	r.logger.Printf(format, args...)
}

func (r *Raft) Trace(format string, args ...interface{}) {
	if r.logLevel > LevelTrace {
		return
	}
	format = fmt.Sprintf("Trace %s", format)
	r.Print(format, args...)
}

func formatLength(s string, l int) (f string) {
	f = s
	for len(f) < l {
		f += " "
	}
	return f
}
