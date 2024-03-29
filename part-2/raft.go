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

// To Enable All Debug logs and we want to keep it this way only for full understanding of RAFT implementation
const DebugCM = 1

// CommitEntry is the data reported by RAFT to the commit channel
// Each commit entry notifies the client that consensus was reached on a command
// and it can be applied to the client's state machine
type CommitEntry struct {
	// Command is the client command being committed
	Command interface{}

	//Index is the log index at which command was committed
	Index int

	//Term is the Raft term at which the client command is committed
	Term int
}

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

	// commitChan is the channel where the CM is going to report committed log
	// entries. It's passed in by the client during construction
	// And by client here we don't mean end-user, its the application (such as database, key-value store)
	// which internally uses RAFT for replicated state machine
	// Write only channel, since won't be listening anything from this channel
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on the commitChan
	newCommitReadyChan chan struct{}

	// Persistent Raft state of all servers
	// Non-Volatile stuff
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile RAFT state on all servers
	commitIndex        int
	lastApplied        int
	state              ConsnensusModuleState
	electionResetEvent time.Time

	// Volatile RAFT state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM
// ReadyChannel signals the CM that all peers arw connected and
// it's safe to start it's state machine
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)

	// Initially every node in the cluster is a follower
	cm.state = Follower
	// When you come up, you haven't voted for anyone yet
	cm.votedFor = -1
	// Initially nothing is committed
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	cm.debugLog("Inside NewConsensusModule id=%d, peerIds=%v", id, peerIds)

	go func() {
		// The CM is dormant until ready is signalled on the ready channel;
		// tgen it starts it countdown for leader election
		<-ready
		cm.mu.Lock() // trying to get a mutex lock before modifying election timer
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	// Invoke this in a different go-routine, so any notification on newCommitReadyChan
	// will be listened by this channel and send committed entries on
	// cm.commitChan
	go cm.commitChanSender()
	return cm
}

// commitChanSender is responsible for sending committed entries on cm.commitChan
// It watches newCommitReadyChan for notifications and calculates which new entries are ready to be
// sent on commitChan channel. This method should run in a separate background goroutine
// cm.commitChan may be buffered and will limit how fast the client consumes new committed entries.
// Returns when a newCommitReadyChan is closed
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// Find which entries we have to apply
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry

		if cm.commitIndex > cm.lastApplied {
			// Why cm.commitIndex+1, because slice last value is not inclusive
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.debugLog("CommitChanSender: entries=%v, savedLastApplied=%d, savedLastAppliedNew=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.debugLog("CommitChanSender done")
}

// Submit submits a new command to the CM. This function doesn't block client
// Clients read the commitChan passed in the constructor to be notified of the committed entries
// It return true iff this CM is the leader - in which case command is accepted
// if false is returned, the client will have to find a different CM to submit this command to
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.debugLog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.debugLog("cm=%+v;Command appended to Log by leader, log=%v", cm, cm.log)
		return true
	}
	return false
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			var reply RequestVoteReply

			cm.debugLog("Sending RequestVote to %d; %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("Received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.debugLog("Someone else became leader or Leader got the reply, hence dropping the reply %+v state=%v", args, cm.state)
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

	// Initializing nextIndex and matchIndex for all the peers of this leader
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.debugLog("Becomes Leader; term=%d, log=%v, nextIndex=%v, matchIndex=%v", cm.currentTerm, cm.log, cm.nextIndex, cm.matchIndex)

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
		go func(peerId int) {
			cm.mu.Lock()
			nextIndex := cm.nextIndex[peerId]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1

			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}

			// All entries to be sent in heartBeat post next index
			entries := cm.log[nextIndex:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()

			cm.debugLog("Sending AppendEntries to %v; nextIndex=%d args=%+v", peerId, nextIndex, args)
			var reply AppendEntriesReply

			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.debugLog("Looks like, I am no longer a leader, someone else became leader; savedCurrentTerm=%d, replyTerm=%d; from peer=%d; appendEntryArgs=%+v", cm.currentTerm, reply.Term, peerId, args)
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						// Where to store the next log for this follower
						cm.nextIndex[peerId] = nextIndex + len(entries)
						// Till where follower has stored the logs
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						cm.debugLog("AppendEntries Reply from %d success: nextIndex := %v, matchIndex=%v", peerId, cm.nextIndex, cm.matchIndex)
						savedCommitIndex := cm.commitIndex

						// Now walk from commitIndex's next index and check till which index you have the most follower
						// then we will commit that particular index
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1 // for self
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex > savedCommitIndex {
							cm.debugLog("Leader is setting commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						// Looks like follower doesn't have right logs, let's try with previous index
						cm.nextIndex[peerId] = nextIndex - 1
						cm.debugLog("AppendEntries reply from %d is not !success nextIndex := %d", peerId, nextIndex-1)
					}
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

// lastLogIndexAndTerm returns the last log index and last log entry's term
// (or -1 if there is no log) for this server
// Expects cm.mu to be locked
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastLogIndex := len(cm.log) - 1
		lastLogIndexTerm := cm.log[lastLogIndex].Term
		return lastLogIndex, lastLogIndexTerm
	} else {
		return -1, -1
	}
}

// Stop stops this CM, cleaning up its state. This method returns quickly
// but it may take a bit of time(up to ~election timeout) for all goroutines
// to exit.
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.debugLog("consensusModule=%+v; Becomes Dead", cm)
	close(cm.newCommitReadyChan)
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

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()

	cm.debugLog("Received RequestVote request: %+v [cmId=%d, currentTerm=%d, votedFor=%d, log index/term=(%d %d)]", args, cm.id, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term ouf of date in RequestVote, args=%+v, currentTerm=%d", args, cm.currentTerm)
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {

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
		cm.debugLog("...Term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	} else if args.Term < cm.currentTerm {
		cm.debugLog("Previous leader might be sending older AppendEntries, lets inform him he is no longer a leader; %+v :[cmId=%d, currentTerm=%d, votedFor=%d]", args, cm.id, cm.currentTerm, cm.votedFor)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches the PrevLogTerm ?
		// In extreme case of PrevLogIndex=-1 this is vacuously true

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a mistmatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent in the RPC
			logInsertIndex := args.PrevLogIndex + 1 // for iterating on cm.log
			newEntriesIndex := 0                    // for iterating on entries received in AppendEntries

			for {
				// Either we crossed the boundries, then break
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}

				// Or we found a mismatch in the term and the log should be replaced
				// from the server's log starting at this point
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// At the end of this loop
			// - logInsertIndex points at the end of log, or an index where the term mis-matches with and entry from leader
			// - newEntriesIndex points at the end of Entries, or an index where the term mismatches witht the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				// Append the remaining entries(from the leader) to the log of this CM
				cm.debugLog("Appending Entries %v to follower %d from index %d ", args.Entries[newEntriesIndex:], cm.id, logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.debugLog("Log is now %v", cm.log)
			}

			// Set commit index
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.debugLog("Setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}
