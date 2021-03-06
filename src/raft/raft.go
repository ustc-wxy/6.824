package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond

	snapshot     []byte
	includeIndex int
	includeTerm  int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartTimer:     time.NewTimer(heartbeatInterval),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.applyCond = sync.NewCond(&rf.mu)
	logLen := len(rf.log)
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, logLen
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	DPrintf("%s is Loading...\n", rf)
	go rf.electionLoop()
	go rf.applyLoop()

	return rf
}
func (rf *Raft) String() string {

	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
		stateArray[rf.state], rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastApplied)
}

/**************************Get() & Set() of *Raft**************************/

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
func (rf *Raft) getLog(index int) *LogEntry {
	log := rf.log
	if len(log) == 0 || log[0].Index > index || log[len(log)-1].Index < index {
		return nil
	}
	for _, v := range log {
		if v.Index == index {
			return &v
		}
	}
	return nil
}
func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}
func (rf *Raft) getLastLog() LogEntry {
	size := len(rf.log)
	return rf.log[size-1]
}

func (rf *Raft) getHigherLogs(startIndex int) []LogEntry {
	var res []LogEntry
	for _, v := range rf.log {
		if v.Index >= startIndex {
			res = append(res, v)
		}
	}
	return res
}

/**********************Change Method of three States***********************/
func (rf *Raft) becomeFollower(term int) {
	oldState := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.electionTimer.Reset(randomElectionTimeout())
	if oldState == Candidate {
		DPrintf("%s change to follower from Candidate\n", rf)
	}
	if oldState == Leader {
		DPrintf("%s change to follower from Leader\n", rf)
	}
}
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("%s change to Candidate\n", rf)
}
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.persist()
	peerSum := len(rf.peers)
	//logLen := len(rf.log)
	for i := 0; i < peerSum; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLog().Index + 1
	}
	rf.heartTimer.Reset(heartbeatInterval)
	go rf.heartBeatLoop()
	DPrintf("%s change to leader\n", rf)
}

/**************************Three essential loops***************************/
/*
	Author:sqdbibibi
	Date:2022/4/20
	FunctionDescription???Leader's toy.
	Part:2A
*/
const heartbeatInterval = time.Duration(50 * time.Millisecond)

func randomElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Intn(150) + 150
	return time.Duration(randomNum) * time.Millisecond
}
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		//send AppendEntries to all peers
		rf.broadCastHeartBeat(true)

		rf.mu.Unlock()

		<-rf.heartTimer.C
		rf.heartTimer.Reset(heartbeatInterval)

	}
}

/*
	Author:sqdbibibi
	Date:2022/4/19
	FunctionDescription???A goroutine which receives the election timeout periodically.
	Part:2A
*/
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		<-rf.electionTimer.C

		rf.mu.Lock()
		rf.electionTimer.Reset(randomElectionTimeout()) //common operator
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		rf.becomeCandidate()
		rf.mu.Unlock()

		rf.startElection()

	}
}

/*
	Author:sqdbibibi
	Date:2022/4/22
	FunctionDescription???A goroutine which commit command to ApplyChan periodically.
	Part:2B
*/
func (rf *Raft) applyLoop() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex := rf.getFirstLog().Index
		rf.lastApplied = Max(rf.lastApplied, firstIndex)
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		DPrintf("%s is Applying! lastApplied is %d,commitIndex is %d\n", rf, rf.lastApplied, rf.commitIndex)

		applyEntires := make([]LogEntry, commitIndex-lastApplied)
		copy(applyEntires, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])

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
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

/**************************AppendEntries Part******************************/

/*
	Author:sqdbibibi
	Date:2022/4/21
	FunctionDescription???send HeartBeat.
	Part:2B
*/
func (rf *Raft) broadCastHeartBeat(isHearBeat bool) {
	//Mutex locked
	for i := range rf.peers {
		if i == rf.me {
			rf.matchIndex[i] = rf.getLastLog().Index
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			continue
		}
		if isHearBeat {
			go rf.singleReplicate(i)
		} else {
			rf.replicatorCond[i].Signal()
		}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/*
	Author:sqdbibibi
	Date:2022/4/21-22
	FunctionDescription???AppendEntries RPC Handler.
	Part:2A 2B
*/
/*
	Author:sqdbibibi
	Date:2022/4/28
	Type:Update,Rewrite snapshot about.
	Part:2D
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s has received AppendEntries %s\n", rf, args)
	LPrintf("%s log is %v\n", rf, rf.log)
	//Step1.Check term and state
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		//DPrintf("%s AppendEntries reply is %v\n", rf, reply)
		return
	}
	rf.becomeFollower(args.Term)

	//Step2.Conflict check
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	prevLog := rf.getLog(prevLogIndex)
	if prevLog == nil {
		return
	}
	if prevLog.Index != prevLogIndex || prevLog.Term != prevLogTerm {
		return
	}

	//Step3.Replicate new log
	for idx, _ := range rf.log {
		if rf.log[idx].Index == prevLog.Index {
			rf.log = append(rf.log[0:idx+1], args.Entries...)
			break
		}
	}

	//Step4.CommitIndex update
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLog().Index)
		rf.applyCond.Signal()
	}
	reply.Success = true
	DPrintf("%s AppendEntries reply is %v\n", rf, reply)
}

/*
	Author:sqdbibibi
	Date:2022/4/22
	FunctionDescription???Leader handle appendEntries RPC Response.
	Part:2B
*/
func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%s handle AppendEntries Response %v\n", rf, reply)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	peer := server
	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		majorIndex := rf.getMajorityIndex()
		entry := rf.getLog(majorIndex)

		if entry == nil {
			return
		}
		if entry.Term == rf.currentTerm && majorIndex > rf.commitIndex {
			rf.commitIndex = majorIndex
			rf.applyCond.Signal()
		}
	} else {
		prevLogIndex := args.PrevLogIndex
		for prevLogIndex > rf.getFirstLog().Index && rf.getLog(prevLogIndex).Term == args.PrevLogTerm {
			prevLogIndex--
		}
		if prevLogIndex == rf.getFirstLog().Index {
			prevLogIndex--
		}
		rf.nextIndex[peer] = prevLogIndex + 1
	}
	//rf.mu.Unlock()
}
func (rf *Raft) getMajorityIndex() int {
	arr := make([]int, len(rf.matchIndex))
	copy(arr, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(arr)))
	return arr[len(arr)/2]
}

/****************************Snapshot Part*********************************/
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("%s has received InstallSnapshot %v\n", rf, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.becomeFollower(args.Term)
	rf.mu.Unlock()

	//outdate snapshot
	if args.LastIncludeIndex <= rf.commitIndex {
		return
	}
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludeTerm,
	}
}
func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s handle Snapshot Response %v\n", rf, reply)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	rf.matchIndex[server] = args.LastIncludeIndex
	rf.nextIndex[server] = args.LastIncludeIndex + 1
}

/*
	Author:sqdbibibi
	Date:2022/4/28
	Type: Update
	FunctionDescription???Add Snapshot case.
	Part:2D
*/
func (rf *Raft) singleReplicate(peer int) {
	//Mutex UnLocked
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[peer]
	prevIndex := nextIndex - 1
	DPrintf("%s prevIndex=%v inIndex=%v\n", rf, prevIndex, rf.includeIndex)
	LPrintf("%s log is %v\n", rf, rf.log)
	if prevIndex < rf.includeIndex {
		//send snapshot
		args := &InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LeaderId:         rf.me,
			LastIncludeIndex: rf.includeIndex,
			LastIncludeTerm:  rf.includeTerm,
			Snapshot:         rf.snapshot,
		}
		reply := &InstallSnapshotReply{}
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshot(peer, args, reply)
		if ok {
			rf.handleInstallSnapshot(peer, args, reply)
		}
	} else {
		//send entries
		prevLog := rf.getLog(prevIndex)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      rf.getHigherLogs(prevLog.Index + 1),
			LeaderCommit: rf.commitIndex,
		}

		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(peer, args, reply)
		if ok {
			rf.handleAppendEntriesResponse(peer, args, reply)
		}

	}

}

/*
	Author:sqdbibibi
	Date:2022/4/24
	FunctionDescription:Optimize the RPC overhead.
	Part:2C
*/
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.singleReplicate(peer)
	}
}
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
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

/*
	Author:sqdbibibi
	Date:2022/4/19
	FunctionDescription???A candidate attempt to start election.
	Part:2A
*/
func (rf *Raft) startElection() {
	//mutex UnLocked.
	rf.mu.Lock()
	cond := sync.NewCond(&rf.mu)
	//step1.prepare RPC Args
	lastLog := rf.getLastLog()
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
				rf.becomeFollower(reply.Term)
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
			} else {
				return
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
			DPrintf("%s has received %d votes,finished is %d\n", rf, voteCount, voteFinished)
			rf.becomeLeader()
		}
		// 4.25 Author:sqdbibibi
		if voteFinished > peerSum/2 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}()

}

/*
	Author:sqdbibibi
	Date:2022/4/18
	Part:2A
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%s has received %v\n", rf, args)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reqTerm := args.Term
	curTerm := rf.currentTerm

	//Step1.Term Update
	if reqTerm < curTerm {
		reply.Term = curTerm
		return
	}
	if reqTerm > curTerm {
		rf.becomeFollower(reqTerm)
	}
	reply.Term = reqTerm

	//Step2.Vote(Rules:Paper Figure.2)
	voteGranted := true
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		voteGranted = false
	}
	lastLog := rf.getLastLog()
	if lastLog.Term > args.LastLogTerm ||
		(lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex) {
		voteGranted = false
	}
	reply.VoteGranted = voteGranted
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
	DPrintf("%s voteGranted is %v, CandidateId is %d\n", rf, reply.VoteGranted, args.CandidateId)
	rf.persist()
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		DPrintf("%s decode Term error!\n", rf)
		log.Fatal(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		DPrintf("%s decode votedFor error!\n", rf)
		log.Fatal(err)
	}
	if err := e.Encode(rf.log); err != nil {
		DPrintf("%s decode Log error!\n", rf)
		log.Fatal(err)
	}
	//
	if err := e.Encode(rf.includeIndex); err != nil {
		DPrintf("%s decode includeIndex error!\n", rf)
		log.Fatal(err)
	}
	if err := e.Encode(rf.includeTerm); err != nil {
		DPrintf("%s decode includeTerm error!\n", rf)
		log.Fatal(err)
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var includeIndex int
	var includeTerm int
	if err := d.Decode(&currentTerm); err != nil {
		DPrintf("%s read Term error!\n", rf)
		log.Fatal(err)
	} else {
		rf.currentTerm = currentTerm
	}
	if err := d.Decode(&votedFor); err != nil {
		DPrintf("%s read votedFor error!\n", rf)
		log.Fatal(err)
	} else {
		rf.votedFor = votedFor
	}
	if err := d.Decode(&logs); err != nil {
		DPrintf("%s read log error!\n", rf)
		log.Fatal(err)
	} else {
		rf.log = logs
	}
	//
	if err := d.Decode(&includeIndex); err != nil {
		DPrintf("%s read includeIndex error!\n", rf)
		log.Fatal(err)
	} else {
		rf.log = logs
	}
	if err := d.Decode(&includeTerm); err != nil {
		DPrintf("%s read includeTerm error!\n", rf)
		log.Fatal(err)
	} else {
		rf.log = logs
	}

}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s does a CondInstallsnapshot.\n", rf)
	if rf.includeIndex > lastIncludedIndex || rf.includeTerm > lastIncludedTerm {
		return false
	}
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.getHigherLogs(lastIncludedIndex)
		rf.log[0].Command = nil
	}
	DPrintf("%s after cond sp\n", rf)
	rf.log[0].Index, rf.log[0].Term = lastIncludedIndex, lastIncludedTerm
	rf.includeIndex, rf.includeTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.snapshot = snapshot
	rf.persist()
	return true
}

/*
	Author:sqdbibibi
	Date:2022/4/28
	Part:2D
*/
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s does a snapshot\n", rf)
	rf.log = rf.getHigherLogs(index)
	tempEntry := rf.log[0]
	rf.log[0].Command = nil
	rf.snapshot = snapshot
	rf.includeIndex = tempEntry.Index
	rf.includeTerm = tempEntry.Term
	DPrintf("%s after a snapshot\n", rf)
	rf.persist()

}

/*
	Author:sqdbibibi
	Date:2022/4/21-22
	FunctionDescription???
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
	//DPrintf("%s has received Command!\n", rf)
	rf.persist()

	rf.broadCastHeartBeat(false)
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//func (rf *Raft) ticker() {
//	for rf.killed() == false {
//	}
//}
