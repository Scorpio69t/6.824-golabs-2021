package raft

const (
	followerHeartbeatMs   = 180
	electionMs            = 2000
	leaderAppendEntriesMs = 110
)

const (
	DefaultLogLevel = LevelNoLog
)

const (
	maxLenNewEntries = 10
)
