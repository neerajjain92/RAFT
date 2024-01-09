package raft

import (
	"log"
	"testing"
	"time"
)

type Harness struct {
	// cluster is a list of all the raft server participating in cluster
	cluster []*Server

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
		servers[serverId] = NewServer(serverId, peerIds, ready)
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

	return &Harness{
		cluster:      servers,
		connected:    connected,
		totalServers: totalServers,
		t:            t,
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
