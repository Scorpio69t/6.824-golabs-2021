package raft

import (
	"fmt"
	"log"
	"os"
)

type raftLogger struct {
	raft *Raft
	log.Logger
	logLevel int
}

const (
	LevelTrace = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelNoLog
)

// mapStringToLogLevel converts string to LogLevel
func mapStringToLogLevel(s string) int {
	switch s {
	case "TRACE":
		return LevelTrace
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARN":
		return LevelWarn
	case "ERROR":
		return LevelError
	case "NO_LOG":
		return LevelNoLog
	default:
		return DefaultLogLevel
	}
}

const (
	EnvKeyLogLevel = "LOG_LEVEL"
)

// newRaftLogger builds raftLogger.
func newRaftLogger(r *Raft) (rl *raftLogger) {
	rl = new(raftLogger)
	rl.Logger = *log.Default()
	rl.Logger.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rl.raft = r
	rl.logLevel = mapStringToLogLevel(os.Getenv(EnvKeyLogLevel))
	return
}

func (rl *raftLogger) Warn(format string, args ...interface{}) {
	if rl.logLevel > LevelWarn {
		return
	}
	format = fmt.Sprintf("WARN  %s", format)
	rl.Print(format, args...)
}

func (rl *raftLogger) Debug(format string, args ...interface{}) {
	if rl.logLevel > LevelDebug {
		return
	}
	format = fmt.Sprintf("DEBUG %s", format)
	rl.Print(format, args...)
}

func (rl *raftLogger) Info(format string, args ...interface{}) {
	if rl.logLevel > LevelInfo {
		return
	}
	format = fmt.Sprintf("INFO  %s", format)
	rl.Print(format, args...)
}

func (rl *raftLogger) Error(format string, args ...interface{}) {
	if rl.logLevel > LevelError {
		return
	}
	format = fmt.Sprintf("ERROR %s", format)
	rl.Print(format, args...)
}

func (rl *raftLogger) Print(format string, args ...interface{}) {
	format = fmt.Sprintf("{id=%d, state=%s, term=%s}: %s", rl.raft.me, formatLengthString(rl.raft.raftState.getState().String(), 9), formatLengthUint64(rl.raft.getCurrentTerm(), 3), format)
	rl.Printf(format, args...)
}

func (rl *raftLogger) Trace(format string, args ...interface{}) {
	if rl.logLevel > LevelTrace {
		return
	}
	format = fmt.Sprintf("TRACE %s", format)
	rl.Print(format, args...)
}

func (rl *raftLogger) LogLevel() int {
	return rl.logLevel
}

func (rl *raftLogger) SetLogLevel(level int) {
	rl.logLevel = level
}

func formatLengthString(s string, l int) (f string) {
	f = s
	for len(f) < l {
		f += " "
	}
	return f
}

func formatLengthUint64(n uint64, l int) (f string) {
	s := fmt.Sprintf("%d", n)
	return formatLengthString(s, l)
}

func formatLogEntries(logEntries []*LogEntry) (s string) {
	for _, entry := range logEntries {
		if len(s) == 0 {
			s = fmt.Sprintf("%d", entry.Term)
		} else {
			s = fmt.Sprintf("%s %d", s, entry.Term)
		}
	}
	return fmt.Sprintf("[%s]", s)
}
