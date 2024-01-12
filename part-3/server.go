// Server container for a Raft consensus Module, Exposes Raft to the network
// and enables RPCs between Raft peers

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server wraps up a raft.ConsensusModule along with a rpc.Server that exposes
// its methods as RPC endpoints. It also manages the peers of the Raft Server.
type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	cm       *ConsensusModule
	storage  Storage
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.storage = storage
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})
	s.commitChan = commitChan
	return s
}

func (server *Server) Serve() {
	server.mu.Lock()
	server.cm = NewConsensusModule(server.serverId, server.peerIds, server, server.storage, server.ready, server.commitChan)

	// Create a new RPC Server and register a RPC Proxy that forwards methods to consensus module
	server.rpcServer = rpc.NewServer()
	server.rpcProxy = &RPCProxy{cm: server.cm}
	server.rpcServer.RegisterName("ConsensusModule", server.rpcProxy)

	var err error
	server.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", server.serverId, server.listener.Addr())
	server.mu.Unlock()

	server.wg.Add(1)
	go func() {
		defer server.wg.Done()

		for {
			conn, err := server.listener.Accept()
			if err != nil {
				select {
				case <-server.quit:
					return
				default:
					log.Fatalf("Error Listeneing on port %s: err=%v", server.listener.Addr(), err)
				}
			}
			server.wg.Add(1)
			go func() {
				server.rpcServer.ServeConn(conn)
				server.wg.Done()
			}()
		}

	}()
}

func (server *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	// log.Printf("Calling Remote Peer [%d] on rpcMethod=[%s]", id, serviceMethod)
	server.mu.Lock()
	peer := server.peerClients[id]
	server.mu.Unlock()

	if peer == nil {
		log.Printf("Call Client %d after it's closed", id)
		return fmt.Errorf("Call Client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

type RPCProxy struct {
	cm *ConsensusModule
}

func (rpc *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// log.Printf("serverId=%d; Inside RequestVote in RPC Proxy...", rpc.cm.id)
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpc.cm.debugLog("Drop RequeestVote")
			return fmt.Errorf("RPC Failed")
		} else if dice == 8 {
			rpc.cm.debugLog("Delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpc.cm.RequestVote(args, reply)
}

func (rpc *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	// log.Printf("serverId=%d; Inside AppendEntries in RPC Proxy...", rpc.cm.id)
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpc.cm.debugLog("Drop AppendEntries")
			return fmt.Errorf("RPC Failed")
		} else if dice == 8 {
			rpc.cm.debugLog("Delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpc.cm.AppendEntries(args, reply)
}

func (rpc *RPCProxy) SayHello() {
	// log.Printf("serverId=%d; Hello from", rpc.cm.id)
}

// Utility Method

// DisconnectAll closes all the connections to peers of this server
func (server *Server) DisconnectAll() {
	server.mu.Lock()
	defer server.mu.Unlock()
	for id := range server.peerClients {
		if server.peerClients[id] != nil {
			server.peerClients[id].Close() // Close the RPC client
			server.peerClients[id] = nil
			log.Printf("Closing the Peer [%d] of server[%d] ", id, server.serverId)
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly
func (server *Server) Shutdown() {
	server.cm.Stop()
	close(server.quit)
	server.listener.Close()
	server.wg.Wait()
}

// GetListenerAddr returns the current listener #net.Addr
func (server *Server) GetListenerAddr() net.Addr {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.listener.Addr()
}

// ConnectToPeer will rpc#dial to the respective peer net.Addr
// and store it's Client object into server#peerClients array into respective index
// We don't re-connect twice if the connection is already established.
func (server *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	server.mu.Lock()
	defer server.mu.Unlock()

	if server.peerClients[peerId] == nil {
		// Connecting to the server for first time
		// log.Printf("serverId=%d; peerId=%d; addr=%v; ConnectingToPeer", server.serverId, peerId, addr.String())
		client, err := rpc.Dial(addr.Network(), addr.String())

		if err != nil {
			return err
		}
		server.peerClients[peerId] = client
	}

	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId
func (server *Server) DisconnectPeer(peerId int) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.peerClients[peerId] != nil {
		err := server.peerClients[peerId].Close()
		server.peerClients[peerId] = nil
		return err
	}
	return nil
}
