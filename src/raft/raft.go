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
	"sort"
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
	Index   int
	Term    int
	Command interface{}
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

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
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
		heartTimer:    time.NewTimer(heartbeatInterval),
		applyCh:       applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	// start ticker goroutine to start elections
	fmt.Printf("%s is Loading...\n", rf)
	go rf.ElectionLoop()
	go rf.ApplyLoop()

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
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) GetLastLog() LogEntry {
	size := len(rf.log)
	return rf.log[size-1]
}

//func (rf *Raft) GetHigherLogs(startIndex int) []LogEntry {
//	var res []LogEntry
//	for i := startIndex; i < len(rf.log); i++ {
//		res = append(res, rf.log[i])
//	}
//	return res
//}

/**********************Change Method of three States***********************/
func (rf *Raft) BecomeFollower(term int) {
	oldState := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	if oldState == Candidate {
		DPrintf("%s change to follower from Candidate\n", rf)
	}
	if oldState == Leader {
		DPrintf("%s change to follower from Leader\n", rf)
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
	peerSum := len(rf.peers)
	logLen := len(rf.log)
	for i := 0; i < peerSum; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = logLen
	}
	rf.heartTimer.Reset(heartbeatInterval)
	go rf.HeartBeatLoop()
	fmt.Printf("%s change to leader\n", rf)
}

/**************************Three essential loops***************************/
/*
	Author:sqdbibibi
	Date:2022/4/20
	FunctionDescription：Leader's toy.
	Part:2A
*/
const heartbeatInterval = time.Duration(50 * time.Millisecond)

func (rf *Raft) HeartBeatLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		//send AppendEntries to all peers
		rf.BroadCastHeartBeat()
		rf.mu.Unlock()

		<-rf.heartTimer.C
		rf.heartTimer.Reset(heartbeatInterval)
		DPrintf("%s start new ping round.", rf)
	}
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

/*
	Author:sqdbibibi
	Date:2022/4/22
	FunctionDescription：A goroutine which commit command to ApplyChan periodically.
	Part:2B
*/
func (rf *Raft) ApplyLoop() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		DPrintf("%s is Applying! lastApplied is %d,commitIndex is %d\n", rf, rf.lastApplied, rf.commitIndex)
		applyEntires := make([]LogEntry, commitIndex-lastApplied)
		copy(applyEntires, rf.log[lastApplied+1:commitIndex+1])

		rf.mu.Unlock()
		//Asychronous apply
		for _, entry := range applyEntires {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("%s applies entries %v-%v\n", rf, rf.lastApplied, commitIndex)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

/**************************AppendEntries Part******************************/

/*
	Author:sqdbibibi
	Date:2022/4/21
	FunctionDescription：send HeartBeat.
	Part:2B
*/
func (rf *Raft) BroadCastHeartBeat() {
	//Mutex locked
	for i := range rf.peers {
		if i == rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = len(rf.log) - 1
			continue
		}
		go rf.SingleReplicate(i)
	}
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

func (ae *AppendEntriesArgs) String() string {
	return fmt.Sprintf("[Term:%d;preIndex:%d;preTerm:%d;Entries:%v;Lcom:%d]",
		ae.Term, ae.PrevLogIndex, ae.PrevLogTerm, ae.Entries, ae.LeaderCommit)
}

func (rf *Raft) SingleReplicate(peer int) {
	//Mutex UnLocked
	rf.mu.Lock()
	//DPrintf("%s ping peer %d\n", rf, peer)
	prevLogIndex := rf.nextIndex[peer] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(peer, args, reply)
	if ok {
		rf.handleAppendEntriesResponse(peer, args, reply)
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/*
	Author:sqdbibibi
	Date:2022/4/21-22
	FunctionDescription：AppendEntries RPC Handler.
	Part:2A 2B
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s has received AppendEntries %s\n", rf, args)
	//Step1.Check term and state
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	rf.BecomeFollower(args.Term)

	//Step2.Conflict check
	prevLogIndex := args.PrevLogIndex
	if len(rf.log) <= prevLogIndex || rf.log[prevLogIndex].Term != args.PrevLogTerm {
		if len(rf.log) > prevLogIndex {
			rf.log = rf.log[0:prevLogIndex]
		}
		return
	}
	//Step3.Replicate new log
	rf.log = append(rf.log[0:prevLogIndex+1], args.Entries...)
	//Step4.CommitIndex update
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}
	reply.Success = true
	DPrintf("%s AppendEntries reply is %v\n", rf, reply)
}

/*
	Author:sqdbibibi
	Date:2022/4/22
	FunctionDescription：Leader handle appendEntries RPC Response.
	Part:2B
*/
func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	peer := server
	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		majorIndex := rf.getMajorityIndex()
		if rf.log[majorIndex].Term == rf.currentTerm && majorIndex > rf.commitIndex {
			//DPrintf("%s majorIndex is %d\n", rf, majorIndex)
			rf.commitIndex = majorIndex
			rf.applyCond.Signal()
		}
	} else {
		prevLogIndex := args.PrevLogIndex
		for prevLogIndex > 0 && rf.log[prevLogIndex].Term == args.PrevLogTerm {
			prevLogIndex--
		}
		rf.nextIndex[peer] = prevLogIndex + 1
	}
	rf.mu.Unlock()
}
func (rf *Raft) getMajorityIndex() int {
	arr := make([]int, len(rf.matchIndex))
	copy(arr, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(arr)))
	return arr[len(arr)/2]
}

/******************************Election Part*******************************/

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
	//mutex UnLocked.
	rf.mu.Lock()
	cond := sync.NewCond(&rf.mu)
	//step1.prepare RPC Args
	lastLog := rf.GetLastLog()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
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

	//Step1.Term Update
	if reqTerm < curTerm {
		return
	}
	if reqTerm > curTerm {
		rf.BecomeFollower(reqTerm)
	}
	reply.Term = reqTerm

	//Step2.Vote(Rules:Paper Figure.2)
	voteGranted := true
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		voteGranted = false
	}
	lastLog := rf.GetLastLog()
	if lastLog.Term > args.LastLogTerm ||
		(lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex) {
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

/*
	Author:sqdbibibi
	Date:2022/4/21-22
	FunctionDescription：
	Part:2B
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	index := -1

	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	logLen := len(rf.log)
	if logLen > 0 {
		index = rf.log[logLen-1].Index + 1
	} else {
		//Note:index start from 0
		index = 1
	}
	rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
	DPrintf("%s has received Command!\n", rf)
	rf.persist()

	rf.BroadCastHeartBeat()
	rf.mu.Unlock()
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
