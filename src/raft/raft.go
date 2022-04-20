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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

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

//
// A Go object implementing a single Raft peer.
//
type PeerState int

const (
	Follower = iota
	Candidate
	Leader
)

var stateArray = [3]string{
	"Follower",
	"Candidate",
	"Leader",
}

type LogEntry struct {
	index   int
	term    int
	command interface{}
}

/*
	Author:sqdbibibi
	Date:2022/4/18
	Part:2A
*/
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	//Persistent state on all servers:
	state       PeerState
	currentTerm int
	votedFor    int
	log         []LogEntry
	//Volatile state on all servers:
	commitIndex int
	lastApplied int
	//Volatile state on all servers:
	nextIndex  []int
	matchIndex []int

	//Two important timer
	electionTimer *time.Timer
	heartTimer    *time.Timer
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]LogEntry, 1),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		electionTimer: time.NewTimer(RandomElectionTimeout()),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	fmt.Printf("%s is Loading...\n", rf)
	go rf.ElectionLoop()
	//go rf.ticker()

	return rf
}
func (rf *Raft) String() string {

	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
		stateArray[rf.state], rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastApplied)
}

/**************************Get() & Set() of *Raft**************************/

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

func (rf *Raft) GetLastLog() LogEntry {
	size := len(rf.log)
	return rf.log[size-1]
}

/**********************Change Method of three States***********************/
func (rf *Raft) BecomeFollower(term int) {
	oldState := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	if oldState == Candidate {
		fmt.Printf("%s change to follower from Candidate\n", rf)
	}
	if oldState == Leader {
		fmt.Printf("%s change to follower from Leader\n", rf)
	}
}
func (rf *Raft) BecomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	fmt.Printf("%s change to Candidate\n", rf)
}
func (rf *Raft) BecomeLeader() {
	rf.state = Leader
	rf.persist()

	go rf.pingLoop()
	fmt.Printf("%s change to leader\n", rf)
}

/**************************Three essential loops***************************/
/*
	Author:sqdbibibi
	Date:2022/4/19
	FunctionDescription：Leader's toy.
	Part:2A
*/
func (rf *Raft) pingLoop() {
	//for {
	//
	//}
}

/*
	Author:sqdbibibi
	Date:2022/4/19
	FunctionDescription：A goroutine which receives the election timeout periodically.
	Part:2A
*/
func (rf *Raft) ElectionLoop() {
	for rf.killed() == false {
		<-rf.electionTimer.C

		rf.mu.Lock()
		rf.electionTimer.Reset(RandomElectionTimeout()) //common operator
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		rf.BecomeCandidate()
		rf.mu.Unlock()

		rf.StartElection()

	}
}
func RandomElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Intn(150) + 150
	return time.Duration(randomNum) * time.Millisecond
}

/*
	Author:sqdbibibi
	Date:2022/4/19
	FunctionDescription：A candidate attempt to start election.
	Part:2A
*/
func (rf *Raft) StartElection() {
	//mutex Locked.
	rf.mu.Lock()
	cond := sync.NewCond(&rf.mu)
	//step1.prepare RPC Args
	lastLog := rf.GetLastLog()
	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLog.index,
		lastLog.term,
	}
	//step2.send RPC to all peers.
	voteCount := 1
	voteFinished := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(x int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(x, args, reply) { //send RPC request to peer i
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.BecomeFollower(reply.Term)
				return
			}
			if reply.Term < rf.currentTerm {
				return
			}
			if rf.state == Candidate {
				if reply.VoteGranted {
					voteCount++
				}
				voteFinished++
			}
			cond.Broadcast()
		}(i)
	}
	rf.mu.Unlock()
	//Step3.gather voter's result
	go func() {
		rf.mu.Lock()
		peerSum := len(rf.peers)
		for voteCount <= peerSum/2 && voteFinished != peerSum {
			cond.Wait()
		}
		if voteCount > peerSum/2 && rf.state == Candidate {
			fmt.Printf("%s has received %d votes,finished is %d\n", rf, voteCount, voteFinished)
			rf.BecomeLeader()
		}
		rf.mu.Unlock()
	}()

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
/*
	Author:sqdbibibi
	Date:2022/4/18
	Part:2A
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("%s has received %v\n", rf, args)

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reqTerm := args.Term
	curTerm := rf.currentTerm
	//state := rf.state

	//Step1.Term Update
	if reqTerm < curTerm {
		return
	}
	if reqTerm > curTerm {
		//if state == Follower {
		//	rf.currentTerm = reqTerm
		//}
		//if state == Candidate || state == Leader {
		rf.BecomeFollower(reqTerm)
		//}
	}
	reply.Term = reqTerm

	//Step2.Vote(Rules:Paper Figure.2)

	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	voteGranted := true
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		voteGranted = false
	}
	lastLog := rf.GetLastLog()
	if lastLog.term > args.LastLogTerm ||
		(lastLog.term == args.LastLogTerm && lastLog.index > args.LastLogIndex) {
		voteGranted = false
	}
	reply.VoteGranted = voteGranted
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
	fmt.Printf("%s voteGranted is %v, CandidateId is %d\n", rf, reply.VoteGranted, args.CandidateId)
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
