package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

// randomTimeoutInt returns an integer that is between the minVal an 2x minVal
func randomTimeoutInt(minVal int) int {
	return minVal + int(rand.Int63() % int64(minVal))
}