package raft

import (
	"fmt"
	"os"
)

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

// initLogLevel return LogLevel based on environment variable LOG_LEVEL.
// If LOG_LEVEL is not found, return DefaultLogLevel.
func initLogLevel() int {
	s := os.Getenv("LOG_LEVEL")
	return mapStringToLogLevel(s)
}

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
	format = fmt.Sprintf("{id=%d, state=%s, term=%s}: %s", r.me, formatLengthString(r.raftState.getState().String(), 9), formatLengthUint64(r.getCurrentTerm(), 3), format)
	r.logger.Printf(format, args...)
}

func (r *Raft) Trace(format string, args ...interface{}) {
	if r.logLevel > LevelTrace {
		return
	}
	format = fmt.Sprintf("TRACE %s", format)
	r.Print(format, args...)
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
