package raft

import (
	"fmt"
)

func (r *Raft) Warn(format string, args ...interface{}) {
	format = fmt.Sprintf("WARN: %s", format)
	r.Print(format, args...)
}

func (r *Raft) Debug(format string, args ...interface{}) {
	if !r.debug {
		return
	}
	format = fmt.Sprintf("DEBUG: %s", format)
	r.Print(format, args...)
}

func (r *Raft) Info(format string, args ...interface{}) {
	format = fmt.Sprintf("INFO: %s", format)
	r.Print(format, args...)
}

func (r *Raft) Error(format string, args ...interface{}) {
	format = fmt.Sprintf("ERROR: %s", format)
	r.Print(format, args...)
}

func (r *Raft) Print(format string, args ...interface{}) {
	format = fmt.Sprintf("{id=%d, state=%s, term=%d}: %s", r.me, r.raftState.getState().String(), r.raftState.getCurrentTerm(), format)
	r.logger.Printf(format, args...)
}
