// CORE Raft Implementation - Consensus Module

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type RequestVoteArgs struct {
	Term         int // The term for which vote is being requested
	CandidateId  int // Id of the follower's Server becoming candiate
	LastLogIndex int // LastLogIndex which this follower has before becoming candidate
	LastLogTerm  int // LastLogTerm which this follower has before becoming candiate
}

type RequestVoteReply struct {
	Term         int  // Term for which vote is being given/denied
	VoteGranted  bool // Whether or not other peer voted for you
	WhoGranted   int  // Who granted/denied this VoteRequest
	WhoRequested int  // Who requested this vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type ConsnensusModuleState int

const (
	Follower ConsnensusModuleState = iota
	Candidate
	Leader
	Dead
)

func (state ConsnensusModuleState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candiate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// Consensus Module implement a single node of Raft consensus
type ConsensusModule struct {
	// mutext protectes concurrent access to consensus module
	mu sync.Mutex

	// id is the server ID of this consensus module
	// Imagine the ID of the server running key-value store
	// which will include this consensus module to utilize raft for consensus
	id int

	// peerIds lists the IDs of our peers in the cluster
	peerIds []int

	// server is the server containing this conesnsusModule,
	// it's used to issue RPC calls to the peers
	server *Server

	// Persistent Raft state of all servers
	// Non-Volatile stuff
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile stuff
	state              ConsnensusModuleState
	electionResetEvent time.Time
}

// NewConsensusModule creates a new CM
// ReadyChannel signals the CM that all peers arw connected and
// it's safe to start it's state machine
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	// Initially every node in the cluster is a follower
	cm.state = Follower
	// When you come up, you haven't voted for anyone yet
	cm.votedFor = -1

	cm.debugLog("Inside NewConsensusModule id=%d, peerIds=%v", id, peerIds)

	go func() {
		// The CM is quiescent until ready is signalled on the ready channel;
		// tgen it starts it countdown for leader election
		<-ready
		cm.mu.Lock() // trying to get a mutex lock before modifying election timer
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

// electionTimeout generates a pseudo-random election timeout duration
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// if RAFT_FORCE_MORE_REELECTION is set, stress-test deliberately
	// generating hard-coded election timeout causing collisions and multiple-relection, split-brain.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		// Give anything between 150 to 300 milliseconds
		// as mentioned in the white paper.
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// runElectionTimer implements an election timer. It should be launched whenever we
// want to start a timer towards becoming a candidate in a new election
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.debugLog("Election Timer started (%v), term = %d", timeoutDuration, termStarted)

	// Loops until either:
	// - We discover that election timer is no longer needed, or
	// - The election timer expiers and this CM(follower) becomes candiate
	// In a follower this typically keeps running in the background for
	// the duration of CM's lifetime
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.debugLog("In Election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.debugLog("In Election Timer, Term Changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Starts an Election if we haven't heard from a leader or haven't
		// voted for someone for the duration of timeout
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.debugLog("timeElapsed=%d; timeoutDuration=%d; Election Time Elapsed", elapsed, timeoutDuration)
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}

}

// startElection startss as a new election with this CM(follower) as a candidate
// Expects cm.mu to be locked
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.debugLog("follower becomes candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	// Voting for self
	votesReceived := 1

	// Send ReequestVote RPC to all other peers concurrently
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}

			var reply RequestVoteReply

			cm.debugLog("Sending RequestVote to %d; %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("Received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.debugLog("Someone else became leader, hence dropping the reply +%v state=%v", args, cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.debugLog("Term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						// To Proove you have majority votes
						// Including yours
						// Assuming you have 5 nodes for majority you need 3 votes for majority
						// So (2 * 3) > len(peers which is 4) + 1(self) (Total 5)
						// We can also write this as voteReceived > len(peerIds)/2
						if votesReceived*2 > len(cm.peerIds)+1 {
							// Won the election
							cm.debugLog("Wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election is not successful
	go cm.runElectionTimer()
}

// becomeFollower makes this CM back a follower from candiate
// Expects cm.mu to be locked
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.debugLog("Becomes follower with term=%d from term=%d; log=%v", term, cm.currentTerm, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// startLeader switches cm from candidate to a leader state and begin sending heart-beats
// Expects cm.mu to be locked
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.debugLog("Becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still a leader
		for {
			cm.leaderSendHeartBeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()

}

// leaderSendHeartBeats sends a round of heartbeats to all peers,
// collects their replies and adjusts CM's state
func (cm *ConsensusModule) leaderSendHeartBeats() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}

		go func(peerId int) {
			cm.debugLog("Sending AppendEntries to %v; args=%+v", peerId, args)
			var reply AppendEntriesReply

			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.debugLog("Term out of date in heartbeat reply savedCurrentTerm=%d, replyTerm=%d", savedCurrentTerm, reply.Term)
					cm.becomeFollower(reply.Term)
				}
			}
		}(peerId)
	}

}

// debugLog logs a debugging message if DebugCM > 0
func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	if DebugCM > 0 {
		// Prefixing logs with respective ConsensusModule's Server Id
		format = fmt.Sprintf("NodeId=[%d] -> -+", cm.id) + format
		log.Printf(format, args...)
	}
}

// Report reports the state of this CM
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// cm.debugLog("Publishing Report id=%d;currentTerm=%d;state=%s", cm.id, cm.currentTerm, cm.state)
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state. This method returns quickly
// but it may take a bit of time(up to ~election timeout) for all goroutines
// to exit.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.debugLog("consensusModule=%+v; Becomes Dead", cm)
}

// Now following functions will be invoked by peers
// AppendEntries
// RequestVote (other peers will invoke this )
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// If this server is dead, let's not do anything
	if cm.state == Dead {
		return nil
	}

	cm.debugLog("Received RequestVote request: %+v [cmId=%d, currentTerm=%d, votedFor=%d]", args, cm.id, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term ouf of date in RequestVote, args=%+v, currentTerm=%d", args, cm.currentTerm)
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		if cm.votedFor == args.CandidateId {
			cm.debugLog("Received RequestVote on Self-Node: requestVoteArgs=%+v", args)
		}
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.WhoGranted = cm.id
	reply.WhoRequested = args.CandidateId
	reply.Term = cm.currentTerm
	// cm.debugLog("... RequestVote reply: %+v", reply)
	return nil // No errors
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.debugLog("Received AppendEntries request: %+v [cmId=%d, currentTerm=%d, votedFor=%d]", args, cm.id, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}
