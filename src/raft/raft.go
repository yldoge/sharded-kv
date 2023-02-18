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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// constants
const (
	MIN_ELECTION_TIMEOUT   = 300
	ELECTION_TIMEOUT_RANGE = 150
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
	role int // server role

	// Persistent state
	currentTerm int // latest term server has seen
	votedFor    int // candidatedID that received vote in current term
	log         Log

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	snapshot []byte

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
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
	DPrintf(dPersist, "S%d T%d VF:%d LOG: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	state := rf.getEncodedStateL()
	rf.persister.SaveRaftState(state)
}

func (rf *Raft) persistSnapshotL() {
	DPrintf(dPersist, "S%d T%d VF%d LOG%v With SS", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	state := rf.getEncodedStateL()
	rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
}

func (rf *Raft) getEncodedStateL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("Error readPersist on server", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf(dSnap, "S%d T%d rejected (LII%d <= CI%d)", rf.me, rf.currentTerm, lastIncludedIndex, rf.commitIndex)
		return false
	}

	rf.snapshot = Clone(snapshot)
	rf.log.compact(lastIncludedIndex, lastIncludedTerm)
	rf.persistSnapshotL()

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	DPrintf(dSnap, "S%d T%d installed LII%d LIT%d", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.getLastIncludedIndex() {
		DPrintf(dSnap, "S%d T%d LII%d reject a stale snapshot[%d]", rf.me, rf.currentTerm, rf.log.getLastIncludedIndex(), index)
		return
	}
	rf.snapshot = Clone(snapshot)
	rf.log.compact(index, rf.log.getTermAt(index))
	rf.persistSnapshotL()
	DPrintf(dSnap, "S%d T%d installed LII%d", rf.me, rf.currentTerm, rf.log.getLastIncludedIndex())
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// ISS is from the current leader
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	rf.electionTimer.Reset(RandElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf(dSnap, "S%d T%d <- L%d rejected InstallSnapshot (args.LII:%d <= CI:%d)", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type Entry struct {
	Command interface{}
	Term    int
}

func (e Entry) String() string {
	return fmt.Sprintf("[T%d]", e.Term)
}

type Log struct {
	Entries           []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func NewLog() Log {
	return Log{make([]Entry, 0), 0, 0}
}

func (l *Log) compact(lastIncludedIndex, lastIncludedTerm int) {
	l.LastIncludedTerm = lastIncludedTerm
	l.trimFront(lastIncludedIndex)
}

// trimFront trim log entries from head through i, keep i + 1 to the end
func (l *Log) trimFront(index int) {
	if index > l.getLastIndex() {
		l.Entries = []Entry{}
	} else {
		l.Entries = l.Entries[index-l.LastIncludedIndex:]
	}
	l.LastIncludedIndex = index
}

// trimEnd trim log entries from i (includes i) to the end
func (l *Log) trimEnd(index int) {
	l.Entries = l.Entries[:index-l.LastIncludedIndex-1]
}

func (l *Log) getTermAt(index int) int {
	if index == l.getLastIncludedIndex() {
		return l.getLastIncludedTerm()
	}
	return l.Entries[index-l.LastIncludedIndex-1].Term
}

// [s, e)
func (l *Log) slice(s, e int) []Entry {
	return l.Entries[s-l.LastIncludedIndex-1 : e-l.LastIncludedIndex-1]
}

func (l *Log) getLastIndex() int {
	return l.LastIncludedIndex + len(l.Entries)
}

func (l *Log) getLastTerm() int {
	if len(l.Entries) == 0 {
		return l.getLastIncludedTerm()
	}
	return l.Entries[len(l.Entries)-1].Term
}

func (l *Log) getStartIndex() int {
	return l.LastIncludedIndex + 1
}

func (l *Log) getLastIncludedIndex() int {
	return l.LastIncludedIndex
}

func (l *Log) getLastIncludedTerm() int {
	return l.LastIncludedTerm
}

func (l *Log) length() int {
	return len(l.Entries) + l.LastIncludedIndex
}

func (l *Log) append(entries []Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (l *Log) appendOne(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l Log) String() string {
	return fmt.Sprintf("LII:%d LIT:%d Entries: %v", l.LastIncludedIndex, l.LastIncludedTerm, l.Entries)
}

// func (l Log) String() string {
// 	return fmt.Sprintf("[last index %d][last term %d][lastIncludedIdx %d][lastIncludedT %d]", l.getLastIndex(), l.getLastTerm(), l.getLastIncludedIndex(), l.getLastIncludedTerm())
// }

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // log entries to store
	LeaderCommit int     // leader's commitIndex
}

func (aea AppendEntriesArgs) String() string {
	return fmt.Sprintf("PLI:%d PLT%d LC%d Entries:%v", aea.PrevLogIndex, aea.PrevLogTerm, aea.LeaderCommit, aea.Entries)
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm

	// For accelerated log backtraking
	ConfilictIndex int
	ConflictTerm   int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf(dTerm, "S%d <- L%d AE NOT OK (S.T%d > L.T%d)", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	// AE is from the current leader
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	rf.electionTimer.Reset(RandElectionTimeout())
	DPrintf(dTimer, "S%d <- L%d reset timeout", rf.me, args.LeaderId)

	pli := args.PrevLogIndex
	plt := args.PrevLogTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if rf.log.getLastIndex() < pli {
		reply.ConfilictIndex = rf.log.length()
		reply.ConflictTerm = -1
		reply.Success = false
		DPrintf(dLog, "S%d <- L%d T%d No PI:%d", rf.me, args.LeaderId, rf.currentTerm, pli)
		return
	}

	lii := rf.log.getLastIncludedIndex()
	// have prevLogIndex, but the term does not match
	if pli > lii && rf.log.getTermAt(pli) != plt {
		reply.ConflictTerm = rf.log.getTermAt(pli)
		ci := rf.log.getStartIndex()
		for rf.log.getTermAt(ci) < reply.ConflictTerm {
			ci++
		}
		reply.ConfilictIndex = ci
		reply.Success = false
		DPrintf(dLog, "S%d <- L%d conflict at PI:%d", rf.me, args.LeaderId, pli)
		return
	}

	reply.Success = true
	if pli < lii {
		DPrintf(dLog, "S%d <- L%d T%d PI:%d in Snapshot(LII%d)", rf.me, args.LeaderId, rf.currentTerm, pli, lii)
		if pli+len(args.Entries) <= lii {
			args.Entries = []Entry{}
		} else {
			args.Entries = args.Entries[lii-pli:]
			pli = lii
		}
	}

	// update log
	if len(args.Entries) > 0 {
		diffAt := -1
		for i, e := range args.Entries {
			if pli+1+i > rf.log.getLastIndex() || e.Term != rf.log.getTermAt(pli+1+i) {
				diffAt = pli + 1 + i
				break
			}
		}
		if diffAt == -1 {
			return
		} else {
			rf.log.trimEnd(diffAt)
			rf.log.append(args.Entries[diffAt-(pli+1):])
			DPrintf(dLog2, "S%d <- L%d %v", rf.me, rf.currentTerm, rf.log)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commit(Min(args.LeaderCommit, rf.log.getLastIndex()))
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

func (rva RequestVoteArgs) String() string {
	return fmt.Sprintf("LLI:%d, LLT:%d", rva.LastLogIndex, rva.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rvr RequestVoteReply) String() string {
	return fmt.Sprintf("[T %d][VoteGranted %v]", rvr.Term, rvr.VoteGranted)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()
	DPrintf(dVote, "S%d <- C%d T%d RV: %v", rf.me, args.CandidateId, args.Term, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(dVote, "S%d T%d > C%d T%d REJECTED", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if rf.log.getLastTerm() > args.LastLogTerm {
		DPrintf(dVote, "S%d <- C%d higher last term (%d > %d) REJECTED", rf.me, args.CandidateId, rf.log.getLastTerm(), args.LastLogTerm)
		return
	}

	if rf.log.getLastTerm() == args.LastLogTerm && rf.log.length() > args.LastLogIndex {
		DPrintf(dVote, "S%d <- C%d log longer (%d > %d) REJECTED", rf.me, args.CandidateId, rf.log.length(), args.LastLogIndex)
		return
	}

	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimer.Reset(RandElectionTimeout())
		DPrintf(dVote, "S%d <- C%d T%d GRANTED", rf.me, args.CandidateId, args.Term)
		DPrintf(dTimer, "S%d T%d reset for vote granted", rf.me, rf.currentTerm)
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
		newLog := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log.appendOne(newLog)
		DPrintf(dClient, "L%d New log: %v", rf.me, rf.log)
		rf.persist()
		index = rf.log.getLastIndex()
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

	return index, term, isLeader
}

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
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.role != leader {
				go rf.elect()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.role == leader {
				rf.heartbeats()
				rf.heartbeatTimer.Reset(HeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.toCandidate()
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.getLastIndex(),
		LastLogTerm:  rf.log.getLastTerm(),
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
			ok := rf.requestVote(server, &args)
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
	defer rf.mu.Unlock()
	for vote*2 <= len(rf.peers) && total != len(rf.peers) {
		cond.Wait()
		DPrintf(dVote, "C%d T%d: %d/%d/%d", rf.me, rf.currentTerm, vote, total, len(rf.peers))
	}
	if vote*2 > len(rf.peers) {
		if rf.role != candidate || rf.currentTerm != args.Term {
			DPrintf(dInfo, "S%d T%d No longer cand (Elect T%d)", rf.me, rf.currentTerm, args.Term)
			return
		}
		rf.toLeader()
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HeartbeatTimeout())
	}
}

func (rf *Raft) requestVote(server int, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	if ok := rf.sendRequestVote(server, args, &reply); ok {
		if reply.VoteGranted {
			return true
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			DPrintf(dInfo, "C%d Step down T(%d -> %d)", rf.me, rf.currentTerm, reply.Term)
			rf.toFollower(reply.Term)
			rf.persist()
		}
	}
	return false
}

func (rf *Raft) heartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.heartbeat(i)
		}
	}
}

func (rf *Raft) heartbeat(fw int) {
	rf.mu.Lock()
	ni := rf.nextIndex[fw]
	if ni < rf.log.getStartIndex() {
		rf.mu.Unlock()
		rf.sendSnapshot(fw)
		return
	}
	pi := ni - 1
	li := rf.log.getLastIndex()
	var entries []Entry
	if li >= ni {
		entries = make([]Entry, li-ni+1)
		copy(entries, rf.log.slice(ni, li+1))
	} else {
		entries = []Entry{}
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: pi,
		PrevLogTerm:  rf.log.getTermAt(pi),
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	DPrintf(dLeader, "L%d -> S%d AE:%v", rf.me, fw, args)
	if ok := rf.sendAppendEntries(fw, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// get old RPC replies
		if rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// logs could have been updated since this RPC
			// make sure these don't roll back due to install snapshot
			rf.matchIndex[fw] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[fw])
			rf.nextIndex[fw] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[fw])
			DPrintf(dLeader, "L%d T%d <- S%d OK MI%d NI%d", rf.me, rf.currentTerm, fw, rf.matchIndex[fw], rf.nextIndex[fw])
			for N := rf.log.getLastIndex(); N > rf.commitIndex; N-- {
				cnt := 0
				for _, mi := range rf.matchIndex {
					if mi >= N {
						cnt++
					}
				}
				if cnt*2 > len(rf.peers) && rf.log.getTermAt(N) == rf.currentTerm {
					rf.commit(N)
					break
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				// step down
				DPrintf(dInfo, "L%d Step down T(%d -> %d)", rf.me, rf.currentTerm, reply.Term)
				rf.toFollower(reply.Term)
				rf.persist()
			} else {
				// fail because log inconsistency (accerlerated log backtracking)
				// should first find the conflict term
				i := rf.log.getLastIndex()
				s := rf.log.getStartIndex()
				for i >= s && rf.log.getTermAt(i) != reply.ConflictTerm {
					i--
				}
				if i >= s {
					// found
					rf.nextIndex[fw] = i + 1
				} else {
					rf.nextIndex[fw] = reply.ConfilictIndex
				}
				DPrintf(dLeader, "L%d <- S%d CONFLICT NI%d", rf.me, fw, rf.nextIndex[fw])
			}
		}
	}
}

func (rf *Raft) sendSnapshot(fw int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.getLastIncludedIndex(),
		LastIncludedTerm:  rf.log.getLastIncludedTerm(),
		Data:              rf.snapshot,
	}
	DPrintf(dSnap, "L%d T%d -> S%d, LII:%d, LIT:%d", rf.me, rf.currentTerm, fw, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(fw, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// get old RPC replies
		if rf.currentTerm > args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			DPrintf(dInfo, "L%d step down T(%d < %d)", rf.me, rf.currentTerm, reply.Term)
			rf.toFollower(reply.Term)
			rf.persist()
			return
		}
		rf.nextIndex[fw] = Max(rf.nextIndex[fw], args.LastIncludedIndex+1)
		rf.matchIndex[fw] = Max(rf.matchIndex[fw], args.LastIncludedIndex)
	}
}

func (rf *Raft) commit(N int) {
	rf.commitIndex = N
	DPrintf(dCommit, "S%d T%d CI%d", rf.me, rf.currentTerm, rf.commitIndex)
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		ci := rf.commitIndex
		si := Max(rf.lastApplied, rf.log.LastIncludedIndex) + 1
		entries := make([]Entry, ci-si+1)
		copy(entries, rf.log.slice(si, ci+1))
		rf.mu.Unlock()
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: si + i,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, ci)
		DPrintf(dClient, "S%d applied %d - %d, LA%d", rf.me, si, ci, rf.lastApplied)
		rf.mu.Unlock()
	}

}

func (rf *Raft) toFollower(newTerm int) {
	DPrintf(dInfo, "S%d update term %d -> T%d, is follower", rf.me, rf.currentTerm, newTerm)
	rf.role = follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) toCandidate() {
	DPrintf(dTimer, "S%d Candidate T%d -> T%d", rf.me, rf.currentTerm, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = candidate
	rf.votedFor = rf.me
	// when start a election, reset election timeout
	rf.electionTimer.Reset(RandElectionTimeout())
}

func (rf *Raft) toLeader() {
	rf.role = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.log.getLastIndex() + 1 // initialized to leader last log index + 1
		rf.matchIndex[i] = 0                        // initialized to 0, increases monotonically
	}
	DPrintf(dLeader, "S%d T%d became LEADER", rf.me, rf.currentTerm)
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
	rf.log = NewLog()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.electionTimer = time.NewTimer(RandElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	DPrintf(dClient, "S%d started", rf.me)
	return rf
}

func RandElectionTimeout() time.Duration {
	return time.Duration(MIN_ELECTION_TIMEOUT+rand.Int63()%ELECTION_TIMEOUT_RANGE) * time.Millisecond
}

func HeartbeatTimeout() time.Duration {
	return time.Duration(HEARTBEATS_TIMEOUT) * time.Millisecond
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func Clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}
