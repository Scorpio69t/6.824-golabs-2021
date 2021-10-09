package raft

import "errors"

// logEntriesManager is used to store and retrieve raft's logs.
type logEntriesManager struct {
	logger    *raftLogger
	raftState *raftState
	logs      []*LogEntry
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
	}
	return
}

var (
	ErrorOutOfRange = errors.New("out of range")
)

// PushLocal appends new log giving the command.
func (lem *logEntriesManager) PushLocal(logEntry *LogEntry) (err error) {
	lem.logs = append(lem.logs, logEntry)
	lem.raftState.setLastLog(logEntry.Index, logEntry.Term)
	return
}

// DeleteAfter deletes the logs after begin.
func (lem *logEntriesManager) DeleteAfter(begin uint64) (err error) {
	prevIndex := begin - 1
	prevLog, _ := lem.Get(prevIndex)
	lem.raftState.setLastLog(prevLog.Index, prevLog.Term)
	lem.logs = lem.logs[:begin]
	return
}

// Get returns the log entry at index.
func (lem *logEntriesManager) Get(index uint64) (log *LogEntry, err error) {
	if index > lem.raftState.getLastIndex() {
		err = ErrorOutOfRange
		return
	}
	log = lem.logs[index]
	return
}

// GetCopiesBetween returns the copy of log entries between begin and end.
func (lem *logEntriesManager) GetCopiesBetween(begin uint64, end uint64) (logs []*LogEntry, err error) {
	end = min(end, lem.raftState.getLastIndex())
	logsRef := lem.logs[begin : end+1]

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
