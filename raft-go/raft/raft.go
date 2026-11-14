package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"

	"lab2raft/labrpc"
	"math/rand"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	// (Updated on stable storage before responding to RPCs)

	currentTerm int         // latest term server has seen
	votedFor    int         // candidateId that received vote in current term
	log         []*LogEntry // log entries; each entry contains command for state machine,
	// and term when entry was received by leader

	// Volatile state on all servers

	commitIndex int  // index of highest log entry known to be committed
	lastApplied int  // index of highest log entry applied to state machine
	isLeader    bool // server belief whether it is leader or not

	// Volatile state on leaders
	// (Reinitialized after election)

	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server

	lastTimestamp   time.Time // timestampt to check for timeout
	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	// partA:

	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry (§5.4)

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	if (rf.votedFor == -1 ||
		rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm >= lastTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	}

}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).

	Term         int         // leader’s term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching
	// prevLogIndex and prevLogTermterm

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. idle leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	rf.resetElectionTimer()
	termMatch := rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	// 2. log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !termMatch || args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		termMatch := rf.log[idx].Term == entry.Term
		// 3. an existing entry conflicts with a new one
		// (same index but different terms)
		if idx < len(rf.log) && !termMatch {
			// delete current and following entries
			rf.log = rf.log[:idx]
		}
		// 4. append any new entries not already in the log
		if idx >= len(rf.log) {
			rf.log = append(rf.log, entry)
			rf.resetElectionTimer()
		}
	}

	// 5. update index of last new entry
	lastEntryIdx := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastEntryIdx)
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)  // zeros
	rf.matchIndex = make([]int, n) // zeros
	for i := range rf.nextIndex {
		// leader's last log index + 1
		rf.nextIndex[i] = len(rf.log)
	}

	rf.resetElectionTimer()
	go rf.electionTick()
	return rf
}

// part 3A helper functions
// heartbeat
func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		// check that the server is a leader aand it is not stale
		if !rf.isLeader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for server := range rf.peers {
			// send heartbeats to all *other* servers
			if server == rf.me {
				continue
			}
			//TODO: in 3B build PrevLogIndex/PrevLogTerm from rf.nextIndex[server]
			// (prev = nextIndex[server]-1) and set Entries to a copy of
			// rf.log[nextIndex[server]:]
			// instead of always sending an empty heartbeat from the end of the log
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log) - 1,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			go func(server int) {
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// if any of the other servers are ahead of current leader
					// transfer leadership
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.isLeader = false
						rf.resetElectionTimer()
						return
					}
					//TODO: in 3B if reply.Success update matchIndex[server] and nextIndex[server], then increment commitIndex
					// if it failed because of log mismatch decrement nextIndex[server] and retry on the next round.
				}
			}(server)
		}
		rf.mu.Unlock()
		time.Sleep((100 * time.Millisecond))

	}

}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// increment current term
	rf.currentTerm++
	currTerm := rf.currentTerm
	// vote for self
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         currTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	votes := 1
	for server := range rf.peers {
		// request votes from all *other* servers
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			// vote denied
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// if any of the other servers are ahead of current leader
			// transfer leadership
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.isLeader = false
				rf.resetElectionTimer()
				return
			}
			// stale replies
			if rf.currentTerm != currTerm || !reply.VoteGranted {
				return
			}
			votes++
			// votes are more than half , winner
			if votes > len(rf.peers)/2 && !rf.isLeader {
				rf.isLeader = true
				n := len(rf.peers)
				rf.nextIndex = make([]int, n)
				rf.matchIndex = make([]int, n) // zeros
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
				}

				go rf.heartbeatLoop(currTerm)

			}
		}(server)
	}

}

// timer that waits on election timeouts
func (rf *Raft) electionTick() {

	for {
		// reroll the timer before the timeout
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if !rf.isLeader && time.Since(rf.lastTimestamp) >= rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
	}

}

// helper function to reset the timer
func (rf *Raft) resetElectionTimer() {
	rf.lastTimestamp = time.Now()
	// set random timeout  200-300ms
	rf.electionTimeout = time.Duration(200+rand.Intn(100)) * time.Millisecond
}
