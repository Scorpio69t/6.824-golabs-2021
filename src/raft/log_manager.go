package raft

import (
	"errors"
	"sync"

	"6.824/labgob"
)

// logEntriesManager is used to store and retrieve raft's logs.
type logEntriesManager struct {
	logger    *raftLogger
	raftState *raftState
	persist   func()

	// Persistent
	logs []*LogEntry

	rwMutex sync.RWMutex
}

// NewLogEntriesManager makes a new logEntriesManager.
func NewLogEntriesManager(r *Raft) (lem *logEntriesManager) {
	lem = &logEntriesManager{
		logger: r.logger,
		logs: []*LogEntry{{
			Index: 0,
			Term:  0,
		}},
		raftState: &r.raftState,
		persist:   r.persist,
	}
	return
}

var (
	ErrorOutOfRange = errors.New("out of range")
)

// PushLocal appends new log giving the command.
func (lem *logEntriesManager) PushLocal(logEntry *LogEntry) (err error) {
	lem.logger.Info("PushLocal: index=%d, term=%d, cmd=%d", logEntry.Index, logEntry.Term, logEntry.Command)

	lem.rwMutex.Lock()
	lem.logs = append(lem.logs, logEntry)
	lem.rwMutex.Unlock()

	lem.raftState.setLastLog(logEntry.Index, logEntry.Term)
	lem.persist()
	return
}

// DeleteAfter deletes the logs after begin.
func (lem *logEntriesManager) DeleteAfter(begin uint64) (err error) {
	lem.logger.Info("DeleteAfter: begin=%d\n", begin)
	prevIndex := begin - 1
	prevLog, _ := lem.Get(prevIndex)
	lem.raftState.setLastLog(prevLog.Index, prevLog.Term)

	lem.rwMutex.Lock()
	lem.logs = lem.logs[:begin]
	lem.rwMutex.Unlock()

	lem.persist()
	return
}

// GetLastLog returns the last log.
func (lem *logEntriesManager) GetLastLog() (log *LogEntry, err error) {
	lem.rwMutex.RLock()
	defer lem.rwMutex.RUnlock()
	log = lem.logs[len(lem.logs)-1]
	return
}

// Get returns the log entry at index.
func (lem *logEntriesManager) Get(index uint64) (log *LogEntry, err error) {
	if index > lem.raftState.getLastIndex() {
		err = ErrorOutOfRange
		return
	}
	lem.rwMutex.RLock()
	log = lem.logs[index]
	lem.rwMutex.RUnlock()
	return
}

// GetCopiesBetween returns the copy of log entries between begin and end.
func (lem *logEntriesManager) GetCopiesBetween(begin uint64, end uint64) (logs []*LogEntry, err error) {
	end = min(end, lem.raftState.getLastIndex())
	lem.rwMutex.RLock()
	logsRef := lem.logs[begin : end+1]
	lem.rwMutex.RUnlock()

	// Copy.
	logs = make([]*LogEntry, len(logsRef))
	for i, logRef := range logsRef {
		logs[i] = &LogEntry{
			Index:   logRef.Index,
			Term:    logRef.Term,
			Command: logRef.Command,
		}
	}
	return
}

// Encode encodes the logs.
func (lem *logEntriesManager) Encode(encoder *labgob.LabEncoder) (err error) {
	lem.rwMutex.RLock()
	err = encoder.Encode(lem.logs)
	lem.rwMutex.RUnlock()
	return
}

func (lem *logEntriesManager) SetLogs(logs []*LogEntry) {
	lem.rwMutex.Lock()
	lem.logs = logs
	lem.rwMutex.Unlock()
}
