package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

type Harness struct {
	mu sync.Mutex

	// cluster is a list of all the raft server participating in cluster
	cluster []*Server

	// commitChans has a channel per server in cluster with the commit channel for that server
	commitChans []chan CommitEntry

	// commits at i holds the sequence of commits made by server i so far.
	// It is populated by goroutine that listens on the corresponding newCommitReady channel
	// Now this is how commits will look like after first command ran which is set 43
	// commits[0] = [CommitEntry{command: 43, Index: 0, Term: 1}]
	// commits[1] = [CommitEntry{command: 43, Index: 0, Term: 1}]
	// commits[2] = [CommitEntry{command: 43, Index: 0, Term: 1}]

	// Now 2nd commit came in which is set 84, Now what will happen, Remember this is a log(AppendOnly) we don't override anything
	// So it will look like this. I am assuming there were no re-election happened, that might not be true in production system
	// but for testing purpose we want to keep the term same and see if the commits indeed happend
	// commits[0] = [CommitEntry{command: 43, Index: 0, Term: 1}, CommitEntry{command: 43, Index: 1, Term: 1}]
	// commits[1] = [CommitEntry{command: 43, Index: 0, Term: 1}, CommitEntry{command: 43, Index: 1, Term: 1}]
	// commits[2] = [CommitEntry{command: 43, Index: 0, Term: 1}, CommitEntry{command: 43, Index: 1, Term: 1}]

	// Now in CheckCommitted when you check the commitsLen for all servers, what you are checking is the length of this array
	// and it should be same at each index
	commits [][]CommitEntry

	// connected contains a bool per server in a cluster, representing whether this server
	// is currently connected to peers (of false, it's partitioned and no messages will pass to/from it)
	connected []bool

	t            *testing.T
	totalServers int
}

// NewHarness creates a new test Harness, initialized with
// totalServers connected to each other
func NewHarness(t *testing.T, totalServers int) *Harness {
	servers := make([]*Server, totalServers)
	connected := make([]bool, totalServers)
	commitChans := make([]chan CommitEntry, totalServers)
	commits := make([][]CommitEntry, totalServers)
	ready := make(chan interface{})

	// Create all Servers in this cluster, assign ids and peerIds
	for serverId := 0; serverId < totalServers; serverId++ {
		peerIds := make([]int, 0)
		for peer := 0; peer < totalServers; peer++ {
			if peer == serverId {
				continue
			}
			peerIds = append(peerIds, peer)
		}
		commitChans[serverId] = make(chan CommitEntry)
		servers[serverId] = NewServer(serverId, peerIds, ready, commitChans[serverId])
		servers[serverId].Serve()
	}

	// Now Connect All Peers to Each Other, (MatchMaking)
	for i := 0; i < totalServers; i++ {
		for j := 0; j < totalServers; j++ {
			if i != j {
				servers[i].ConnectToPeer(j, servers[j].GetListenerAddr())
			}
		}
		connected[i] = true
	}

	close(ready)

	h := &Harness{
		cluster:      servers,
		commitChans:  commitChans,
		commits:      commits,
		connected:    connected,
		totalServers: totalServers,
		t:            t,
	}

	for i := 0; i < totalServers; i++ {
		go h.CollectCommits(i)
	}

	return h
}

// collectCommits reads channel commitChans[i] and add all the received entries tot the corresponding
// commits[i]. It's blocking and should be run in a sepearate go-routine.
// It returns when commmitChans[i] is closed
func (harness *Harness) CollectCommits(i int) {
	for commit := range harness.commitChans[i] {
		harness.mu.Lock()
		tlog("Collectng Commits for(%d) got %+v", i, commit)
		harness.commits[i] = append(harness.commits[i], commit)
		harness.mu.Unlock()
	}
}

// Shutdown shuts down all the servers in the harness and waits for them
// to stop running
func (harness *Harness) Shutdown() {
	for i := 0; i < harness.totalServers; i++ {
		harness.cluster[i].DisconnectAll()
	}
	for i := 0; i < harness.totalServers; i++ {
		harness.cluster[i].Shutdown()
	}
}

// CheckSingleLeader checks that only a single server thinks it's the leader
// Returns the leader's ID and TERM, It retrieves serveral times if no leader
// is identified yet
func (harness *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1

		for i := 0; i < harness.totalServers; i++ {
			if harness.connected[i] {
				_, term, isLeader := harness.cluster[i].cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						harness.t.Fatalf("both %d and %d think they are leaders", leaderId, i)
					}
				}
			}
		}

		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	harness.t.Fatalf("leader not found")
	return -1, -1
}

// DisconnectPeer disconnects a server from all other servers in the cluster
func (harness *Harness) DisconnectPeer(id int) {
	tlog("Disconnecting %d", id)
	harness.cluster[id].DisconnectAll()
	for j := 0; j < harness.totalServers; j++ {
		if j != id {
			harness.cluster[j].DisconnectPeer(id)
		}
	}
	harness.connected[id] = false
}

func (harness *Harness) Reconnectpeer(id int) {
	tlog("Reconnecting %d", id)
	for i := 0; i < harness.totalServers; i++ {
		if i != id {
			if err := harness.cluster[id].ConnectToPeer(i, harness.cluster[i].GetListenerAddr()); err != nil {
				harness.t.Fatal(err)
			}
			if err := harness.cluster[i].ConnectToPeer(id, harness.cluster[id].GetListenerAddr()); err != nil {
				harness.t.Fatal(err)
			}
		}
	}
	harness.connected[id] = true
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func SleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// CheckNoLeader checks that no connected server considers itself as the leader
func (harness *Harness) CheckNoLeader() {
	for i := 0; i < harness.totalServers; i++ {
		if harness.connected[i] {
			_, _, isLeader := harness.cluster[i].cm.Report()
			if isLeader {
				harness.t.Fatalf("Server %d is a leader, Want None", i)
			}
		}
	}
}

func (harness *Harness) SubmitToServer(serverId int, command interface{}) bool {
	return harness.cluster[serverId].cm.Submit(command)
}

// CheckCommittedN verifies that command was committed by exactly n connected servers
func (harness *Harness) CheckCommittedN(command int, n int) {
	serversHavingThisCommandCommitted, _ := harness.CheckCommitted(command)
	if serversHavingThisCommandCommitted != n {
		harness.t.Errorf("CheckCommittedN got serversHavingThisCommandCommitted=%d; expected=%d", serversHavingThisCommandCommitted, n)
	}
}

// CheckCommitted verifies that all connected servers have command commmitted at the same index
// It also verifies that all commands before command in the commit sequence matches
// For this to work for now, all commands should be distinct
// Returns the number of servers that have this command committed and it's log index

// TODO: this check may be too strict, Consider that a server can commit something
// and crash before notifying the channel, it's a valid commit but the checker will fail
// because it may not match other servers. This scenario is described in the paper...

func (harness *Harness) CheckCommitted(command int) (numberOfServers int, index int) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	commitsLen := -1 // Initially we don't know how much commands have been committed, lets find out
	// and then simply validate accross all servers

	for serverIndex := 0; serverIndex < harness.totalServers; serverIndex++ {
		if harness.connected[serverIndex] {
			if commitsLen == -1 {
				// We don't know the length, so lets take it from any server
				commitsLen = len(harness.commits[serverIndex]) // Checking length commits of specific server
			} else {
				// This is where we want to check if some commits are missing
				if len(harness.commits[serverIndex]) != commitsLen {
					harness.t.Fatalf("CommitsLength did not match at server=%d; Expected=%d; Got=%d", serverIndex, commitsLen, len(harness.commits[serverIndex]))
				}
			}
		}
	}

	// Check consistency of the commits accross all servers from start to end
	for commitIndex := 0; commitIndex < commitsLen; commitIndex++ {
		commandAtCommitIndex := -1
		for serverIdx := 0; serverIdx < harness.totalServers; serverIdx++ {
			if harness.connected[serverIdx] {
				command := harness.commits[serverIdx][commitIndex].Command.(int)
				if commandAtCommitIndex == -1 {
					// Assuming our clients won't be sending -1 as command to RAFT
					commandAtCommitIndex = command
				} else {
					if commandAtCommitIndex != command {
						harness.t.Fatalf("Command at index [%d] for server [%d] not matching, Expected=%d, Got=%d", commitIndex, serverIdx, commandAtCommitIndex, harness.commits[serverIdx][commitIndex].Command.(int))
					}
				}
			}
		}

		// Checking the consistency of the Index of the command
		if commandAtCommitIndex == command {
			index := -1
			serversHavingThisCommandCommitted := 0
			for serverIdx := 0; serverIdx < harness.totalServers; serverIdx++ {
				if harness.connected[serverIdx] {
					if index == -1 {
						index = harness.commits[serverIdx][commitIndex].Index
					} else {
						if index != harness.commits[serverIdx][commitIndex].Index {
							harness.t.Fatalf("Inconsistency in Index for server[%d]; commitIndex[%d]; IndexExpected=%d; IndexReceived=%d", serverIdx, commitIndex, index, harness.commits[serverIdx][commitIndex].Index)
						}
					}
					serversHavingThisCommandCommitted++
				}
			}
			return serversHavingThisCommandCommitted, index
		}
	}

	// if we reached here that means we didn't find thre command we were looking for
	harness.t.Errorf("cmd=%d not found in commits; commits=%+v", command, harness.commits)
	return -1, -1
}

// CheckNotCommitted verifies that the given command
// has not been committed by any of the active servers yet
func (harness *Harness) CheckNotCommitted(cmd int) {
	harness.mu.Lock()
	defer harness.mu.Unlock()

	for serverIdx := 0; serverIdx < harness.totalServers; serverIdx++ {
		if harness.connected[serverIdx] {
			for commitIndex := 0; commitIndex < len(harness.commits[serverIdx]); commitIndex++ {
				existingCmd := harness.commits[serverIdx][commitIndex].Command
				if existingCmd == cmd {
					harness.t.Errorf("found %d at commits[%d][%d], expected none", cmd, serverIdx, commitIndex)
				}
			}
		}
	}

}
