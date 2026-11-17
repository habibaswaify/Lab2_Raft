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
	"sync/atomic"
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
    applyCh chan ApplyMsg // channel to notify committed entries
	replicateNow chan struct{}
	dead int32 // Added for Kill implementation

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
	//Reject requests from older terms
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//checks whether the candidate has a newer term than this server.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	//Section 5.4 election restriction check
	// Candidate's log is at least as up-to-date if:
	// - candidate's last log term > receiver's last log term, OR
	// - terms are equal and candidate's last log index >= receiver's last log index
	isLogUpToDate := false
	if args.LastLogTerm > lastTerm {
		isLogUpToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
		isLogUpToDate = true
	}
	// The server has not voted for another candidate in the term, or has already voted for this same candidate.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

}
//1. Sending heartbeats to Followers. 2.Replicating log entries from the Leader to Followers.
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

	//outdated leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// Checks whether the leader's term is newer.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	reply.Term = rf.currentTerm
	rf.resetElectionTimer()

	// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		//an existing entry conflicts with a new one
		// (same index but different terms)
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				// delete current and following entries
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
		} else {
			//append any new entries not already in the log
			rf.log = append(rf.log, entry)
		}
	}

	// update index of last new entry
	lastEntryIdx := args.PrevLogIndex + len(args.Entries)//Calculates the last index covered by the request.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastEntryIdx)
	}
	reply.Success = true

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

// Start() is called when an application wants Raft to begin agreeing on a new command.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()

    term := rf.currentTerm

    // Only the leader can accept new commands.
    if !rf.isLeader {
        rf.mu.Unlock()
        return -1, term, false
    }
    //Append the new command to the leader's log.
    newEntry := &LogEntry{
        Command: command,
        Term:    term,
    }
    rf.log = append(rf.log, newEntry)

    //Get the index of the newly appended entry.
    index := len(rf.log) - 1

    // Update the leader's own replication trackers.
    rf.matchIndex[rf.me] = index
    rf.nextIndex[rf.me] = index + 1

    rf.mu.Unlock()

    // Signal the replication loop.
    select {
    case rf.replicateNow <- struct{}{}:
    default:
    }

    //Return immediately without waiting for replication.
    return index, term, true
}



// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// Check whether this Raft instance has been killed.
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
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
	rf.peers = peers//RPC endpoints for every server.
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{{Term: 0}}
	rf.replicateNow = make(chan struct{}, 1)
	rf.applyCh = applyCh

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
	go rf.applier() // start commit applier goroutine
	return rf
}
// Background goroutine to apply committed log entries to applyCh
func (rf *Raft) applier() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(10 * time.Millisecond)

		var entriesToApply []ApplyMsg

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied].Command,
			}
			entriesToApply = append(entriesToApply, msg)
		}
		rf.mu.Unlock()

		//Send committed entries over channel WITHOUT holding rf.mu lock
		for _, msg := range entriesToApply {
			if rf.killed() {
				return
			}
			rf.applyCh <- msg
		}
	}
}
// part 3A helper functions
// heartbeat

func (rf *Raft) heartbeatLoop(term int) {
    for {
		if rf.killed() {
			return
		}
        rf.mu.Lock()
        if !rf.isLeader || rf.currentTerm != term {
            rf.mu.Unlock()
            return
        }

        for server := range rf.peers {
            if server == rf.me { //Checks whether the current server is the leader itself
                continue
            }
            // Find the next entry to send
            next := rf.nextIndex[server]
            prev := next - 1

            // Prepare AppendEntries arguments
            args := &AppendEntriesArgs{
                Term:         rf.currentTerm,
                LeaderId:     rf.me,
                PrevLogIndex: prev,
                PrevLogTerm:  rf.log[prev].Term,
                Entries:      append([]*LogEntry(nil), rf.log[next:]...),
                LeaderCommit: rf.commitIndex,
            }
            go func(server int, args *AppendEntriesArgs) {
				if rf.killed() {
					return
				}				
                reply := &AppendEntriesReply{}

                // Send AppendEntries RPC
                if !rf.sendAppendEntries(server, args, reply) {
                    return
                }

                rf.mu.Lock()
                defer rf.mu.Unlock()
				if rf.killed() {
					return
				}				
				//Checks whether the follower reported a newer term than the leader currently knows
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.votedFor = -1
                    rf.isLeader = false //Steps down from leadership.
                    rf.resetElectionTimer()
                    return
                }
				//Ignore responses from old leadership periods
                if !rf.isLeader || rf.currentTerm != args.Term {
                    return
                }
				if reply.Success {
					// Last entry confirmed by this RPC
					lastReplicated := args.PrevLogIndex + len(args.Entries)

					// Update matchIndex
					if lastReplicated > rf.matchIndex[server] {
						rf.matchIndex[server] = lastReplicated
					}

					// Update nextIndex
					if lastReplicated+1 > rf.nextIndex[server] {
						rf.nextIndex[server] = lastReplicated + 1
					}

					//Check if a majority has replicated an entry
					rf.advanceCommitIndexLocked()
				// failed replictions
				}  else {


                    // Ignore outdated failure responses
                    if rf.nextIndex[server] == args.PrevLogIndex+1 &&
                        rf.matchIndex[server] < args.PrevLogIndex && //Checks that the leader has not already confirmed replication through the requested preceding entry
                        rf.nextIndex[server] > 1 {

                        // Move backward to find a matching entry
                        rf.nextIndex[server]--

                        // Trigger another replication attempt
                        select {
                        case rf.replicateNow <- struct{}{}:
                        default:
                        }
                    }
                }

            }(server, args)
        }

        rf.mu.Unlock()

        select {
        case <-rf.replicateNow:
            // New command or retry requested
        case <-time.After(100 * time.Millisecond):
            // Periodic heartbeat
        }
    }
}



func (rf *Raft) advanceCommitIndexLocked() {

    //Only the leader can calculate commitment this way
    if !rf.isLeader {
        return
    }

    // 2. Check log entries from newest to oldest
    for N := len(rf.log) - 1; N > rf.commitIndex; N-- {

        // Only count entries from the current term
        if rf.log[N].Term != rf.currentTerm {
            continue
        }

        // 4. Count servers that replicated entry N
        count := 0

        for i := range rf.peers {
            if rf.matchIndex[i] >= N {
                count++
            }
        }

        // Check if a majority replicated entry N
        if count > len(rf.peers)/2 {

            // Advance the commit index
            rf.commitIndex = N

            // Notify followers of the new commit index
            select {
            case rf.replicateNow <- struct{}{}:
            default:
            }

            break
        }
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
			if rf.killed() {
				return
			}			
			reply := &RequestVoteReply{}
			// vote denied
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() {
				return
			}			
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

				// Initialize replication trackers
				rf.nextIndex = make([]int, n)
				rf.matchIndex = make([]int, n)

				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}

				// Leader already has its own log entries
				rf.matchIndex[rf.me] = len(rf.log) - 1

				go rf.heartbeatLoop(currTerm)
			}
		}(server)
	}
}

// timer that waits on election timeouts
func (rf *Raft) electionTick() {
	for {
		if rf.killed() {
			return
		}
		// reroll the timer before the timeout
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		// Only nonleaders should start elections.
		if !rf.isLeader && time.Since(rf.lastTimestamp) >= rf.electionTimeout { //Checks whether enough time has passed since the timer was last reset.
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
