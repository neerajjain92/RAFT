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

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeaderId, _ := harness.CheckSingleLeader()
	tlog("submitting 42 to %d", originalLeaderId)

	isLeader := harness.SubmitToServer(originalLeaderId, 42)
	if !isLeader {
		t.Errorf("want id=%d as leader, but it's not", originalLeaderId)
	}

	SleepMs(150)
	tlog("Checking Committed to N Servers")
	harness.CheckCommittedN(42, 3)
}

func TestSubmitNonLeaderFails(t *testing.T) {
	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	originalLeader, _ := harness.CheckSingleLeader()
	follower := (originalLeader + 1) % 3

	tlog("Submitting to follower %d", follower)
	isLeader := harness.SubmitToServer(follower, 89)
	if isLeader {
		t.Errorf("follower=%d should not be the leader, but it was", follower)
	}
	SleepMs(10)
}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	origLeader, _ := harness.CheckSingleLeader()

	values := []int{40, 50, 60}
	for _, v := range values {
		tlog("Submitting command %d to %d", v, origLeader)
		isLeader := harness.SubmitToServer(origLeader, v)
		if !isLeader {
			t.Errorf("want id=%d as leader, but it's not", origLeader)
		}
		SleepMs(100)
	}

	SleepMs(150)

	serversHavingThisCommandCommitted, index := harness.CheckCommitted(40)
	_, index2 := harness.CheckCommitted(50)
	_, index3 := harness.CheckCommitted(60)

	if serversHavingThisCommandCommitted != 3 {
		t.Errorf("Got serversHavingThisCommandCommitted=%d; expected=%d", serversHavingThisCommandCommitted, 3)
	}

	if index >= index2 {
		t.Errorf("Index [%d] of first command %d, can't be greater than second command [%d], command [%d]", index, 40, index2, 50)
	}

	if index2 >= index3 {
		t.Errorf("Index [%d] of second command %d, can't be greater than third command [%d], command [%d]", index2, 50, index3, 60)
	}
}

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 3)
	defer harness.Shutdown()

	origLeader, _ := harness.CheckSingleLeader()
	follower := (origLeader + 1) % 3

	harness.SubmitToServer(origLeader, 40)
	harness.SubmitToServer(origLeader, 50)

	SleepMs(250)

	harness.CheckCommittedN(40, 3)
	harness.CheckCommittedN(50, 3)

	harness.DisconnectPeer(follower)
	SleepMs(250)

	// Submit a new command, it should not be replicated to disconnected peer/follower
	harness.SubmitToServer(origLeader, 60)
	SleepMs(250)
	harness.CheckCommittedN(60, 2)

	// Now reconnect the peer, and wait a bit, it should have all the command
	harness.Reconnectpeer(follower)
	SleepMs(250)
	harness.CheckSingleLeader()

	SleepMs(150)
	harness.CheckCommittedN(60, 3)
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	harness := NewHarness(t, 5)
	defer harness.Shutdown()

	// Submit couple of values to fully connected cluster
	origLeader, _ := harness.CheckSingleLeader()
	harness.SubmitToServer(origLeader, 5)
	harness.SubmitToServer(origLeader, 6)

	SleepMs(100)
	harness.CheckCommittedN(6, 5)

	// Disconnect the leader
	harness.DisconnectPeer(origLeader)
	SleepMs(10)

	// Submit 7 to original leader, even though it's disconnected
	harness.SubmitToServer(origLeader, 7)
	SleepMs(20)

	harness.CheckNotCommitted(7)

	newLeaderId, _ := harness.CheckSingleLeader()

	// Submit 8 to new leader
	harness.SubmitToServer(newLeaderId, 8)
	SleepMs(150)
	harness.CheckCommittedN(8, 4)

	// Reconnecting the old leadr and let it settle,
	// but regardless Old leader should not be winning due to missed logs
	harness.Reconnectpeer(origLeader)
	SleepMs(600) // Setting it much larger for re-election cycle pass by and logs get correctly replicated

	finalLeaderId, _ := harness.CheckSingleLeader()

	if finalLeaderId == origLeader {
		t.Errorf("Original Leader %d can't be the finalLeader %d since it missed commits", origLeader, finalLeaderId)
	}

	// Submits 9 and check it's fully committed
	harness.SubmitToServer(finalLeaderId, 9)
	SleepMs(200)
	harness.CheckCommittedN(9, 5)
	harness.CheckCommittedN(8, 5)

	// But 7 will not be committed and reverted since it only came to disconnected peer
	harness.CheckNotCommitted(7)
}
