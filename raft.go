package raft

import (
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Command interface{}
	Term    int
}

// enum for states
type State int

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex // Lock to protect shared access to this peer's state
	peers []Peer     // RPC end points of all peers
	me    int        // this peer's index into peers[]

	currentTerm int
	votedFor    int // -1 implies nil
	log         []logEntry
	commitIndex int
	lastApplied int

	// leader state
	nextIndex  []int
	matchIndex []int

	// candidate state
	curVoteCount int

	// extra state
	state State

	// channels
	voteCh            chan bool
	appendEntriesCh   chan bool
	convertToFollower chan bool
	convertToLeader   chan bool
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// lazy locking, change this later
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower <- true
	}
	votedForCheck := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	upToDateCheck := rf.log[rf.commitIndex].Term < args.LastLogTerm || (rf.log[rf.commitIndex].Term == args.LastLogTerm && rf.commitIndex <= args.LastLogIndex)
	if votedForCheck && upToDateCheck {
		reply.VoteGranted = true
		rf.voteCh <- true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if raft is still Candidate, term is still the same, and that the RPC is for this term
	if rf.state != Candidate || args.Term != rf.currentTerm && reply.Term < rf.currentTerm {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.convertToFollower <- true
	}
	if reply.VoteGranted {
		rf.curVoteCount += 1
		if rf.curVoteCount == (len(rf.peers)/2 + 1) {
			rf.convertToLeader <- true
		}
	}
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

func Make(peers []Peer, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	go rf.eventLoop()

	return rf
}

func (rf *Raft) eventLoop() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-rf.voteCh:
			case <-rf.appendEntriesCh:
			case <-time.After(getRandomTimeout()):
				// convert to candidate
			}
		case Candidate:
			select {
			case <-rf.convertToFollower:
				// convert to follower
			case <-rf.convertToLeader:
				// convert to leader
			case <-time.After(getRandomTimeout()):
				// convert to candidate
			}
		case Leader:
			select {
			case <-rf.convertToFollower:
				// convert to follower
			case <-time.After(120 * time.Millisecond):
				// heartbeat
			}
		}
	}
}

func getRandomTimeout() time.Duration {
	return time.Duration(300 + rand.Intn(200))
}
