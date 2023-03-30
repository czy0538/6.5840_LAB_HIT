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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	HEARTBEAT = 110 // ms
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

type Votes struct {
	mu    sync.Mutex
	term  int
	votes int
}

func (v *Votes) Add(term int) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.term != term {
		return -1
	}
	v.votes++
	return v.votes
}

func (v *Votes) Start(term int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.votes = 1
	v.term = term
}

type Entry struct {
	command interface{}
	term    int
}

// Raft
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

	// state for all server
	currentTerm int // latest term server has seen (initialized to 1 on first boot, increases monotonically)
	// TODO: don't forget to update votedFor when update currentTerm
	//votedFor int     // candidateId that received vote in current term (or -1 if none)
	votedFor map[int]int // candidateId that received vote in all term (or 0 if none)
	log      []Entry     // log entries; each entry contains command for state machine,and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	myState         int8 // 0 follower, 1 candidate, 2 leader
	lastAppliedTime time.Time
	vote            Votes
}

func (rf *Raft) changeTerm(term int) {
	rf.currentTerm = term
	//rf.votedFor = -1
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.myState == 2 {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // erm of candidate’s last log entry
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// Reply false if term < currentTerm (§5.1)
	request1 := rf.currentTerm <= args.Term
	// If votedFor is null or candidateId
	request2 := rf.votedFor[args.Term] == 0 || rf.votedFor[args.Term] == args.CandidateId
	// candidate’s log is at least as up-to-date as receiver’s log,
	request3 := args.LastLogTerm > rf.log[len(rf.log)-1].term ||
		args.LastLogTerm == rf.log[len(rf.log)-1].term && args.LastLogIndex >= len(rf.log)-1
	logPrint(rf.me, "RequestVote", fmt.Sprintf("rqeuset 1 2 3 %v %v %v", request1, request2, request3))
	if request1 && request2 && request3 {
		reply.VoteGranted = true
		rf.votedFor[args.Term] = args.CandidateId
	}
	logPrint(rf.me, "RequestVote", fmt.Sprintf("%d ask for vote,term is %d,rf.voteFor is %d,voteGranted=%v", args.CandidateId, args.Term, rf.votedFor[args.Term], reply.VoteGranted))
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	if reply.VoteGranted { // win election

		// detect whether current term is as same as send term
		votes := rf.vote.Add(args.Term)
		logPrint(rf.me, " sendRequestVote", fmt.Sprintf("get vote term %d,total ticks is %d", reply.Term, votes))
		// do something if we win the election
		if votes > len(rf.peers)/2 {
			rf.mu.Lock()
			if rf.myState == 2 {
				rf.mu.Unlock()
				return
			}
			rf.myState = 2
			for i, _ := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: len(rf.log) - 1,
						PrevLogTerm:  rf.log[len(rf.log)-1].term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}, &AppendEntriesReply{})
				}
			}
			rf.mu.Unlock()

		}
	}

	return
}

// Both AppendEntries struct and function defined by myself

// AppendEntriesArgs example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int           // leader’s term
	LeaderId     int           // so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PrevLogTerm  int           // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           // leader’s commitIndex
}

// AppendEntriesReply example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// sendAppendEntries, only can be used by leader
// TODO: just do something for 2A
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	// Your code here (2A, 2B).
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	return
}

// AppendEntries handler
// TODO: just do something for 2A
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	logPrint(rf.me, "AppendEntries", fmt.Sprintf("received AppendEntriesArgs, from %d,term is %d", args.LeaderId, args.Term))
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	if rf.myState == 1 && args.Term >= rf.currentTerm || args.Term > rf.currentTerm {
		rf.myState = 0
		rf.changeTerm(args.Term)

	}

	// update lastAppliedTime after confirm term
	rf.lastAppliedTime = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex  whose term matches prevLogTerm (§5.3)
	if len(rf.log)-1 < args.PrevLogIndex {
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogTerm]
	}

	// HeartBeat
	if args.Entries == nil {
		reply.Success = true
		return
	}

	// Append any new entries not already in the log
	for e := range args.Entries {
		rf.log = append(rf.log, Entry{
			command: e,
			term:    args.Term,
		})
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
	}
}

// Start the service using Raft (e.g. a k/v server) wants to start
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

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

// leader election ticker
func (rf *Raft) ticker() {
	// pause for a random amount of time between TIMEOUT+100 and 500 ms
	ms := HEARTBEAT + 100 + (rand.Int63() % 500)
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		t := time.Now().Sub(rf.lastAppliedTime).Milliseconds()
		// if this peer is leader, no need to start an election
		if rf.myState == 2 {
			rf.mu.Unlock()
			goto SLEEP
		}

		// if we already get message in this duration, continue
		logPrint(rf.me, "ticker", fmt.Sprintf("term:%d,ms:%d,t:%d", rf.currentTerm, ms, t))
		if t < ms {
			rf.mu.Unlock()
			goto SLEEP
		}
		// no matter we are a candidate or a follower, we need to start a new election
		rf.mu.Unlock()
		go rf.startElection()
	SLEEP:
		ms = HEARTBEAT + 50 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// leader heartbeat, if current peer isn't leader, it should stop the goroutine
func (rf *Raft) heartbeat() {

	for rf.killed() == false {

		rf.mu.Lock()
		if rf.myState == 2 {
			logPrint(rf.me, "heartbeat", "")
			for i, _ := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: len(rf.log) - 1,
						PrevLogTerm:  rf.log[len(rf.log)-1].term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}, &AppendEntriesReply{})
				}
			}
		}
		rf.mu.Unlock()
		// sleep for a while
		time.Sleep(HEARTBEAT * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	logPrint(rf.me, "startElection", "")
	rf.mu.Lock()
	rf.myState = 1                      // change state to candidate
	rf.currentTerm += 1                 // increase term
	rf.votedFor[rf.currentTerm] = rf.me // vote to myself
	rf.vote.Start(rf.currentTerm)
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1, // start with 1
				LastLogTerm:  rf.log[len(rf.log)-1].term,
			}, &RequestVoteReply{})
		}
	}
	rf.mu.Unlock()
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.currentTerm = 1

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = make(map[int]int)
	rf.log = append(rf.log, Entry{}) // keep the log begin with 1
	rf.lastAppliedTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func logPrint(me int, fname, msg string) {
	//log.Printf("peer %d,function %s,msg:%s\n", me, fname, msg)
}
