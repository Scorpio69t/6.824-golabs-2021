package raft

const (
	followerMinElectionTimeoutMs = 300
	followerMaxElectionTimeoutMs = 450
	candidateElectionMs          = 1500
	leaderAppendEntriesMs        = 100
)

const (
	DefaultLogLevel = LevelNoLog
)

const (
	maxLenNewEntries = 10
)
