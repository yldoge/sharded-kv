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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// constants
const (
	MIN_ELECTION_TIMEOUT   = 400
	MAX_ELECTION_TIMEOUT   = 4000
	ELECTION_TIMEOUT_RANGE = 300
	HEARTBEATS_TIMEOUT     = 100
)

const (
	leader = iota
	follower
	candidate
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leaderId int
	role     int // server role

	// Persistent state
	currentTerm int        // latest term server has seen
	votedFor    int        // candidatedID that received vote in current term
	log         []LogEntry // log entries

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// As followers
	lastHeard time.Time // the last time at which the peer heard from the leader

	// dependency injected
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == leader)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// LogEntry is the log entry structure.
type LogEntry struct {
	Command interface{}
	Term    int
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AE][server %d] has a higher [term %d] than [%d]", rf.me, rf.currentTerm, args.Term)
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.lastHeard = time.Now()
		DPrintf("[AE][server %d] received from [leader %d] conflict on [PrevIndex %d]", rf.me, args.LeaderId, args.PrevLogIndex)
		return
	}

	// update log
	if len(rf.log)-1 > args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) > 0 {
		DPrintf("[LOG][AE][server %d][term %d][role %d]: %v, %+v", rf.me, rf.currentTerm, rf.role, rf.log, args)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commit(args.LeaderCommit)
		} else {
			rf.commit(len(rf.log) - 1)
		}
	}
	if args.Term >= rf.currentTerm {
		rf.toFollower(args.Term)
		rf.leaderId = args.LeaderId
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("[SENT AE][server %d][term %d] sent AE to [server %d]", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("server %d received request vote\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[RV][server %d] has a higher [term %d] than candidate", rf.me, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		DPrintf("[RV][server %d] has an update-to-date log entry. [candidate term: %d]", rf.me, args.Term)
		return
	}

	if rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) > args.LastLogIndex+1 {
		DPrintf("[RV][server %d] has an update-to-date log entry. [candidate term: %d]", rf.me, args.Term)
		return
	}

	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[RV][server %d] granted vote at [term %d]", rf.me, rf.currentTerm)
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
	DPrintf("[SENT RV][server %d][term %d] requested vote from [server %d]", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == leader
	term = rf.currentTerm

	if isLeader {
		newLog := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, newLog)
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		DPrintf("[LOG][start][server %d][term %d][role %d][commit %d]: %v", rf.me, rf.currentTerm, rf.role, rf.commitIndex, rf.log)
	}

	return index, term, isLeader
}

// func (rf *Raft) replicateLog() {
// 	vote := 1
// 	cond := sync.NewCond(&rf.mu)
// 	for i, next := range rf.nextIndex {
// 		if i == rf.me {
// 			continue
// 		}
// 		if len(rf.log)-1 >= next {
// 			go func(server int) {
// 				rf.mu.Lock()
// 				var ok bool
// 				for !ok {
// 					ok = rf.tryLogReplicate(server)
// 				}
// 				rf.mu.Lock()
// 				defer rf.mu.Unlock()
// 				vote++
// 				cond.Broadcast()
// 			}(i)
// 		}
// 	}
// 	rf.mu.Lock()
// 	for vote*2 <= len(rf.peers) {
// 		cond.Wait()
// 	}
// 	rf.mu.Unlock()
// }

// func (rf *Raft) tryLogReplicate(server int) bool {
// 	rf.mu.Lock()
// 	prevLogIndex := rf.nextIndex[server] - 1
// 	args := AppendEntriesArgs{
// 		Term:         rf.currentTerm,
// 		LeaderId:     rf.me,
// 		PrevLogIndex: prevLogIndex,
// 		PrevLogTerm:  rf.log[prevLogIndex].Term,
// 		Entries:      rf.log[rf.nextIndex[server]:],
// 		LeaderCommit: rf.commitIndex,
// 	}
// 	rf.mu.Unlock()
// 	reply := AppendEntriesReply{}

// 	if ok := rf.sendAppendEntries(server, &args, &reply); ok {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()
// 		if reply.Success {
// 			rf.nextIndex[server] = len(rf.log)
// 			// logs could have been updated since this RPC
// 			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
// 			return true
// 		} else {
// 			rf.nextIndex[server]--
// 		}
// 	}
// 	return false
// }

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeoutInt := MIN_ELECTION_TIMEOUT + rand.Intn(ELECTION_TIMEOUT_RANGE)
		timeout := time.Duration(timeoutInt)
		s := time.Now()
		time.Sleep(timeout * time.Millisecond)
		rf.mu.Lock()
		if rf.lastHeard.After(s) || rf.role == leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		// election timeout, start a new election
		go rf.elect()
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.toCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)
	vote := 1 // already voted for itself
	total := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			ok := rf.candidateReqVote(server, &args)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				vote++
			}
			total++
			cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	for vote*2 <= len(rf.peers) && total != len(rf.peers) {
		cond.Wait()
		DPrintf("[VOTE][candidate %d][term %d]: %d/%d/%d", rf.me, rf.currentTerm, vote, total, len(rf.peers))
	}
	if vote*2 > len(rf.peers) {
		if rf.role != candidate || rf.currentTerm != args.Term {
			return
		}
		rf.toLeader()
		rf.leaderId = rf.me
		go rf.heartbeats()
	}
	rf.mu.Unlock()
}

func (rf *Raft) candidateReqVote(server int, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	if ok := rf.sendRequestVote(server, args, &reply); ok {
		if reply.VoteGranted {
			return true
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			DPrintf("[elect][candidate %d][term %d] step back to follower", rf.me, rf.currentTerm)
			rf.toFollower(reply.Term)
		}
	}
	return false
}

func (rf *Raft) heartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader {
			DPrintf("[HB][leader %d][term %d] is alive", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.heartbeat(i)
			}
			time.Sleep(time.Duration(HEARTBEATS_TIMEOUT) * time.Millisecond)
		} else {
			DPrintf("[HB] server %d stop hearbeats", rf.me)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) heartbeat(fw int) {
	rf.mu.Lock()
	ni := rf.nextIndex[fw]
	pi := ni - 1
	// deep copy
	entries := make([]LogEntry, len(rf.log[ni:]))
	copy(entries, rf.log[ni:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: pi,
		PrevLogTerm:  rf.log[pi].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(fw, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// get old RPC replies
		if rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// logs could have been updated since this RPC
			rf.matchIndex[fw] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[fw] = rf.matchIndex[fw] + 1

			for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
				cnt := 0
				for _, mi := range rf.matchIndex {
					if mi >= N {
						cnt++
					}
				}
				if cnt*2 > len(rf.peers) {
					rf.commit(N)
					break
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				// step down
				rf.toFollower(reply.Term)
				DPrintf("[ROLE][server %d] step down at [term %d]", rf.me, rf.currentTerm)
			} else {
				// fail because log inconsistency
				rf.nextIndex[fw]--
			}
		}
	}
}

func (rf *Raft) commit(N int) {
	rf.commitIndex = N
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) toFollower(newTerm int) {
	// DPrintf("[ROLE][server %d][term %d][role %d] became [follower].", rf.me, newTerm, rf.role)
	rf.role = follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastHeard = time.Now()
}

func (rf *Raft) toCandidate() {
	DPrintf("[ROLE][server %d][term %d][role %d] became [candidate].", rf.me, rf.currentTerm+1, rf.role)
	rf.currentTerm++
	rf.role = candidate
	rf.votedFor = rf.me
	rf.lastHeard = time.Now()
}

func (rf *Raft) toLeader() {
	DPrintf("[ROLE][server %d][term %d][role %d] became [leader].", rf.me, rf.currentTerm, rf.role)
	rf.role = leader
	rf.lastHeard = time.Now()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = follower
	rf.currentTerm = 0 // initialized to 0 on first boot, increases monotonically
	rf.votedFor = -1   // -1 is null
	rf.log = []LogEntry{{Term: 0, Command: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("[Make][%d] Initialized.", rf.me)
	return rf
}
