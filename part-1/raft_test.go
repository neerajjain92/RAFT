package raft

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElectionBasic(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	leaderId, leaderTerm := harness.CheckSingleLeader()
	assert.NotEqual(t, -1, leaderId)
	assert.NotEqual(t, -1, leaderTerm)
	log.Printf("leaderId=%d;leaderTerm=%d; TestElectionBasicResult", leaderId, leaderId)
}
