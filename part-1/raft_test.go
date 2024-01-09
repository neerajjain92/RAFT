package raft

import (
	"log"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
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

func TestElectionLeaderDisconnect(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeaderId, originalLeaderTerm := harness.CheckSingleLeader()

	harness.DisconnectPeer(originalLeaderId)
	SleepMs(350)

	newLederId, newTerm := harness.CheckSingleLeader()
	assert.NotEqual(t, newLederId, originalLeaderId, "NewLeader can't be same as originalLeader, Since originalLeader is disonnected")
	assert.NotEqual(t, newTerm, originalLeaderTerm, "NewTerm can't be same as originalLeaderTerm, because of re-election")
}

func TestElectionLeaderAndAnotherDisconnectWithNoQuorum(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeader, _ := harness.CheckSingleLeader()

	harness.DisconnectPeer(originalLeader)

	// Find Next Peer to disconnect
	anotherPeer := (originalLeader + 1) % 3
	harness.DisconnectPeer(anotherPeer)

	// Now hopefully there is no quorum, All you see is the RequestVote being sent from one of the server
	harness.CheckNoLeader()

	// Reconnect one server back, Now we have Quorum
	harness.Reconnectpeer(anotherPeer)
	harness.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	SleepMs(100)
	// Disconnect All Servers from the start
	for i := 0; i < 3; i++ {
		harness.DisconnectPeer(i)
	}

	SleepMs(450)
	harness.CheckNoLeader()

	// Reconnec all servers. A leader will be found
	for i := 0; i < 3; i++ {
		harness.Reconnectpeer(i)
	}
	harness.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeaderId, originalTerm := harness.CheckSingleLeader()
	harness.DisconnectPeer(originalLeaderId)

	SleepMs(350)
	newLeader, newTerm := harness.CheckSingleLeader()

	log.Printf("leader=%d; term=%d; NewLeader result", newLeader, newTerm)
	harness.Reconnectpeer(originalLeaderId)
	SleepMs(150)

	// The older leader should not be the leader, since time has passed and new term might be present
	leader, term := harness.CheckSingleLeader()
	log.Printf("leader=%d; term=%d; TestElectionLeaderDisconnectThenReconnect result", leader, term)
	assert.NotEqual(t, originalLeaderId, leader, "The new leader shouldn't be the reconnected original leader")
	assert.NotEqual(t, originalTerm, newTerm, "The new term shouldn't be the reconnected original leader's term")

}

func TestElectionLeaderDisconnectThenReconnectWith5Members(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 5)
	defer harness.Shutdown()

	originalLeader, _ := harness.CheckSingleLeader()
	harness.DisconnectPeer(originalLeader)
	SleepMs(150)
	newLeader, newTerm := harness.CheckSingleLeader()

	harness.Reconnectpeer(originalLeader)
	SleepMs(150)

	againLeaderId, againTerm := harness.CheckSingleLeader()

	if newLeader != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeader)
	}

	if newTerm != againTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionFollowerDisconnectsAndComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeaderId, originalTerm := harness.CheckSingleLeader()
	otherFollowerId := (originalLeaderId + 1) % 3
	harness.DisconnectPeer(otherFollowerId)

	SleepMs(650)
	harness.Reconnectpeer(otherFollowerId)
	SleepMs(150)

	newLeader, newTerm := harness.CheckSingleLeader()

	log.Printf("oldLeader=%d, oldTerm=%d; newLeader=%d, newTerm=%d", originalLeaderId, originalTerm, newLeader, newTerm)
	if newTerm <= originalTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, originalTerm)
	}
}

func TestElectionDisconnectInLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderId, _ := harness.CheckSingleLeader()

		harness.DisconnectPeer(leaderId)
		otherFollower := (leaderId + 1) % 3
		harness.DisconnectPeer(otherFollower)
		SleepMs(310)

		// No Quorum
		harness.CheckNoLeader()

		// Reconnects Both
		harness.Reconnectpeer(otherFollower)
		harness.Reconnectpeer(leaderId)

		// Give it time to settle
		SleepMs(150)
		harness.CheckSingleLeader()
	}
}
