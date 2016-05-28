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

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "strconv"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	Logs        []Log

	CommitIndex int
	LastApplied int

	state string

	NextIndex  []int
	matchIndex []int

	commandCH chan interface{} // used in leader state

	heartbeatCH chan bool

	BecomeLeaderCH chan bool // used in candidiate state
	voteCount      int       // used in candidate state, reset everytime when became a candidate

}

type Log struct {
	Command interface{}
	Term    int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here.
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return rf.CurrentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastApplied)
	e.Encode(rf.NextIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.Logs)
	d.Decode(&rf.CommitIndex)
	d.Decode(&rf.LastApplied)
	d.Decode(&rf.NextIndex)
	rf.state = "follower"
	rf.VotedFor = -1
	if len(rf.Logs) == 0 {
		firstLog := new(Log) // initialize all nodes' Logs
		rf.Logs = []Log{*firstLog}
	}
	println("rf.me " + strconv.Itoa(rf.me) + " : In readPersist, len(rf.logs): " + strconv.Itoa(len(rf.Logs)) + " Term: " + strconv.Itoa(rf.CurrentTerm))
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	TERM        int
	CANDIDATEID int
	LASTLOGIDX  int
	LASTLOGTERM int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	TERM        int
	VOTEGRANTED bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// Vote granted in following cases:
	// 1. candidate's term is more up-to-date
	// 2. candidate's term is equal to voter's term "AND" one of the following condition satisfied:
	// 		* voter has not voted yet
	// 		* voter has voted candidate at least once. (since each voter can only vote for one guy in each term)
	// 3. Prevent a candidate from winning an election unless its log contains all committed entries.
	//		* the RPC includes information about the candidate's log, and the voter denies its vote if its own log is more
	//		  up-to-date than that of the candidate.

	// Raft determines which of two Logs is more up-to-date by comparing the index and the term of the last entries
	// in the log. If the Logs have last entries with different terms, then the log with the later term is
	// more up-to-date. If the Logs end with the same term, then whichever log is longer is more up-to-date

	rLastLogIdx := len(rf.Logs) - 1
	rLastLogTm := rf.Logs[rLastLogIdx].Term

	if rf.state == "leader" && len(rf.Logs) - 1 >= args.LASTLOGIDX {
		reply.TERM = rf.CurrentTerm
		reply.VOTEGRANTED = false
		return
	}

	if rf.CurrentTerm > args.TERM && len(rf.Logs) - 1 >= args.LASTLOGIDX {
		// println(strconv.Itoa(rf.me) + " REJECT " +  strconv.Itoa(args.CANDIDATEID))
		reply.TERM = rf.CurrentTerm
		reply.VOTEGRANTED = false
		return
	} 

	if rf.CurrentTerm == args.TERM && rf.VotedFor != -1 && rf.VotedFor != args.CANDIDATEID{
		println("meet disagreement.....")
		reply.TERM = rf.CurrentTerm
		reply.VOTEGRANTED = false
		return
	}

	if rf.CurrentTerm < args.TERM {
		rf.mu.Lock()
		rf.state = "follower"
		rf.CurrentTerm = args.TERM
		rf.mu.Unlock()
	} 

	moreUptoDate := ReqMoreUpToDate(rLastLogIdx, rLastLogTm, args.LASTLOGIDX, args.LASTLOGTERM)
	if moreUptoDate {
		// println("rf.me: " + strconv.Itoa(rf.me) + " Term: " + strconv.Itoa(rf.CurrentTerm) + "  ooooooooooo  rf.VotedFor: " + strconv.Itoa(args.CANDIDATEID) + " args.Term: " + strconv.Itoa(args.TERM))
		rf.mu.Lock()
		rf.VotedFor = args.CANDIDATEID
		rf.state = "follower"
		rf.CurrentTerm = args.TERM
		rf.mu.Unlock()
		reply.TERM = rf.CurrentTerm
		reply.VOTEGRANTED = true
		return
	} else {
		// println(strconv.Itoa(args.CANDIDATEID) + " is not more UP-TO-DATE and REJECT by " + strconv.Itoa(rf.me))
		reply.TERM = rf.CurrentTerm
		reply.VOTEGRANTED = false
		return
	}
}

// return true if candidate's log is more up-to-date
func ReqMoreUpToDate(rLastLogIdx int, rLastLogTm int, cLastLogIdx int, cLastLogTm int) bool {
	if rLastLogTm == cLastLogTm {
		return cLastLogIdx >= rLastLogIdx
	} else {
		return cLastLogTm > rLastLogTm
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has granted for the candidate,
	// if so then check if candidate's voteCount has reached majority, if so then switch to "leader" state
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VOTEGRANTED {
			rf.mu.Lock()
			rf.voteCount = rf.voteCount + 1
			println("rf.me: " + strconv.Itoa(rf.me) + " voteFrom: " + strconv.Itoa(server) + " voteCount: " + strconv.Itoa(rf.voteCount))
			rf.mu.Unlock()
			if rf.state == "candidate" && rf.voteCount > len(rf.peers)/2 {
				rf.BecomeLeaderCH <- true
			}
		} else if reply.TERM > rf.CurrentTerm {
			rf.mu.Lock()
			rf.CurrentTerm = reply.TERM
			rf.state = "follower"
			rf.mu.Unlock()
			println(strconv.Itoa(server) + " reject " + strconv.Itoa(rf.me) + " and " + strconv.Itoa(rf.me) + " step down as follower")
		}
	} 
	return ok
}

// send RequestVote to all other nodes in the cluster
func (rf *Raft) BroadcastRequestVote() {

	args := &RequestVoteArgs{}
	

	args.CANDIDATEID = rf.me
	args.TERM = rf.CurrentTerm // current candidate's term.
	args.LASTLOGIDX = len(rf.Logs) - 1
	args.LASTLOGTERM = rf.Logs[args.LASTLOGIDX].Term

	gochan := make(chan int, len(rf.peers)-1)

	// send to all other nodes in parallel
	for k := 0; k < len(rf.peers); k++ {
		if k != rf.me && rf.state == "candidate" { // exclude self
			gochan <- k
			go func() {
				reply := &RequestVoteReply{}
				reply.TERM = rf.CurrentTerm
				temp := <-gochan
				rf.sendRequestVote(temp, *args, reply)
			}()
		}
	}
}

// used by leader of the cluster

type AppendEntries struct {
	TERM         int
	LEADERID     int
	PREVLOGINDEX int
	PREVLOGTERM  int
	ENTRIES      []Log
	LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM      int
	NEXTINDEX int // nextindex to start when AppendEntriesRPC call to a peer, return value only when ACCEPT=true
	ACCEPT    bool
}

func (rf *Raft) AppendEntriesRPC(args AppendEntries, reply *AppendEntriesReply) {

	// AppendEntriesRPC implementation
	// Update CurrentTerm before any further works

	rf.heartbeatCH <- true // hear from heartbeat
	if rf.CurrentTerm > args.TERM && rf.CommitIndex >= args.LEADERCOMMIT {
		reply.TERM = rf.CurrentTerm
		reply.ACCEPT = false
		reply.NEXTINDEX = len(rf.Logs) - 1
		return
	}
	if rf.CurrentTerm < args.TERM {
		rf.CurrentTerm = args.TERM
	}
	if rf.state == "candidate" {
		rf.state = "follower"
	}
	
	reply.TERM = rf.CurrentTerm
	// check index and term
	if len(rf.Logs) > args.PREVLOGINDEX && rf.Logs[args.PREVLOGINDEX].Term == args.PREVLOGTERM {
		rf.mu.Lock()
		// * If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		// * Append any new entries not already in the log
		rf.Logs = rf.Logs[:args.PREVLOGINDEX+1] // include value at index: args.PREVLOGINDEX
		for i := 0; i < len(args.ENTRIES); i++ {
			rf.Logs = append(rf.Logs, args.ENTRIES[i])
		}
		if len(args.ENTRIES) > 0{
			// println(strconv.Itoa(rf.me) + " APPEND NEW LOGS (len: " + strconv.Itoa(len(args.ENTRIES)) + ") from leader " + strconv.Itoa(args.LEADERID) + " now len(rf.Logs): " + strconv.Itoa(len(rf.Logs)))
		}
		// If LEADERCOMMIT > CommitIndex, set CommitIndex = min(LEADERCOMMIT, index of last new entry)
		
		if rf.CommitIndex < args.LEADERCOMMIT {
			if args.LEADERCOMMIT < len(rf.Logs)-1 {
				rf.CommitIndex = args.LEADERCOMMIT
			} else {
				rf.CommitIndex = len(rf.Logs) - 1
			}
		}

		rf.mu.Unlock()
		go rf.persist()
		reply.NEXTINDEX = len(rf.Logs) - 1
		reply.ACCEPT = true
	} else {
		// Reply false if log doesn't contain an entry at PREVLOGINDEX whose term matches PREVLOGTERM
		// println(strconv.Itoa(rf.me) + " append from " + strconv.Itoa(args.LEADERID) + "    FAILED!!!!!!!!!!!!!" + " len(rf.Logs): " + strconv.Itoa(len(rf.Logs)) + " args.PREVLOGINDEX: " + strconv.Itoa(args.PREVLOGINDEX))
		reply.NEXTINDEX = args.PREVLOGINDEX - 1
		reply.ACCEPT = false
	}

	return
}

func (rf *Raft) sendAppendEntriesRPC(server int, args AppendEntries, reply *AppendEntriesReply) bool {
	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has accepted as a leader,
	// if not, check if leader's term is up-to-date, if not, step down to follower
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	if ok {
		rf.mu.Lock()
		rf.NextIndex[server] = reply.NEXTINDEX
		rf.mu.Unlock()
		// println(" SEND TO " + strconv.Itoa(server) + " SUCCESS!!!!!! leader: " + strconv.Itoa(rf.me))
		if reply.ACCEPT == false && reply.TERM > rf.CurrentTerm {
			rf.mu.Lock()
			rf.CurrentTerm = reply.TERM
			println(strconv.Itoa(rf.me) + " step down as follower, with CommitIndex: " + strconv.Itoa(rf.CommitIndex))
			rf.state = "follower"
			rf.mu.Unlock()
		}
	} else {
		// println(" SEND TO " + strconv.Itoa(server) + " FAILED leader: " + strconv.Itoa(rf.me))
		rf.NextIndex[server] = 0
	}
	return ok
}

// send AppendEntriesRPC to all other nodes in the cluster
func (rf *Raft) BroadcastAppendEntriesRPC() {
	// gochan := make(chan int, len(rf.peers)-1)
	for k := 0; k < len(rf.peers); k++ {
		if k != rf.me && rf.state == "leader"{
			args := &AppendEntries{}
			reply := &AppendEntriesReply{}
			args.TERM = rf.CurrentTerm
			args.LEADERID = rf.me
			args.LEADERCOMMIT = rf.CommitIndex
			args.PREVLOGINDEX = rf.NextIndex[k]
			args.PREVLOGTERM = rf.Logs[args.PREVLOGINDEX].Term // term NextIndex of peer
			args.ENTRIES = rf.Logs[args.PREVLOGINDEX+1:]
			rf.sendAppendEntriesRPC(k, *args, reply)
		}
	}
	go rf.persist()
}

// leader update CommitIndex
func (rf *Raft) UpdateCommit() {

	rf.mu.Lock()
	newCommit := rf.CommitIndex
	count := 0
	// * count values which are bigger than old CommitIndex
	// * find the smallest value which is bigger than old CommitIndex
	for i := 0; i < len(rf.NextIndex); i++ {
		if rf.NextIndex[i] > rf.CommitIndex {
			count++
			if newCommit == rf.CommitIndex || newCommit > rf.NextIndex[i] {
				newCommit = rf.NextIndex[i]
			}
		}
	}

	if count > len(rf.peers)/2 && rf.state == "leader"{
		rf.CommitIndex = newCommit
		println("leader " + strconv.Itoa(rf.me) + " , new CommitIndex: " + strconv.Itoa(rf.CommitIndex) + " LastApplied: " + strconv.Itoa(rf.LastApplied) + " len(rf.Logs): " + strconv.Itoa(len(rf.Logs)))
	}
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.CommitIndex
	term := rf.CurrentTerm
	var isLeader bool
	if rf.state == "leader" {
		entry := new(Log)
		entry.Command = command
		entry.Term = rf.CurrentTerm
		rf.mu.Lock()
		rf.Logs = append(rf.Logs, *entry) // append new entry from client
		index = len(rf.Logs) - 1
		rf.NextIndex[rf.me] = index
		rf.mu.Unlock()
		// send to peers ASAP
		gochan := make(chan int, len(rf.peers)-1)
		
		for k := 0; k < len(rf.peers); k++ {
			if k != rf.me && rf.state == "leader" {
				gochan <- k
				go func() {
					temp := <-gochan
					args := &AppendEntries{}
					args.TERM = rf.CurrentTerm
					args.LEADERID = rf.me
					args.LEADERCOMMIT = rf.CommitIndex
					reply := &AppendEntriesReply{}
					args.PREVLOGINDEX = rf.NextIndex[temp]
					args.PREVLOGTERM = rf.Logs[args.PREVLOGINDEX].Term // term NextIndex of peer
					args.ENTRIES = rf.Logs[args.PREVLOGINDEX+1:]
					rf.sendAppendEntriesRPC(temp, *args, reply)
				}()
			}
		}
		// println(strconv.Itoa(rf.me) + " HEAR NEW COMMAND!!!!!!!!HEAR NEW COMMAND!!!!!!!!HEAR NEW COMMAND!!!!!!!! now len(rf.Logs): " + strconv.Itoa(len(rf.Logs)))
		isLeader = true
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Run as a goroutine whenever a node has been initialized.
func (rf *Raft) Loop() {
	// Set out as a follower

	TimeOutConst := 0
	for {
		TimeOutConst = ElectionTimeoutConst()
		if rf.state == "follower" {
			// DO FOLLOWER STUFF
			select {
			case <-rf.heartbeatCH:
			case <-time.After(time.Duration(TimeOutConst) * time.Millisecond):
				if rf.state != "leader" {
					rf.mu.Lock()
					println(strconv.Itoa(rf.me) + " panic, term: " + strconv.Itoa(rf.CurrentTerm) + " len(rf.Logs): " + strconv.Itoa(len(rf.Logs)) + " voteCount: " + strconv.Itoa(rf.voteCount))
					rf.state = "candidate"
					rf.mu.Unlock()
				}
			}
		} else if rf.state == "candidate" {
			// DO CANDIDIATE STUFF
			rf.CandidateState(TimeOutConst)
		} else {
			// DO LEADER STUFF
			rf.LeaderState()
		}
	}
}

// feed newly committed commands into state machine
func (rf *Raft) FeedStateMachine(applyCh chan ApplyMsg) {
	for {
		if rf.state == "leader" {
			rf.UpdateCommit()
		}
		time.Sleep(30 * time.Millisecond)
		if rf.LastApplied < rf.CommitIndex {
			go func() {
				rf.mu.Lock()
				oldApplied := rf.LastApplied
				commitIdx := rf.CommitIndex
				rf.LastApplied = commitIdx
				rf.mu.Unlock()
				if len(rf.Logs)-1 < commitIdx {
					return
				}
				time.Sleep(10 * time.Millisecond)
				for i := oldApplied+1; i <= commitIdx; i++ {
					Msg := new(ApplyMsg)
					Msg.Index = i
					Msg.Command = rf.Logs[i].Command
					applyCh <- *Msg
				}
			}()
		}
	}
}

func (rf *Raft) CandidateState(TimeOutConst int) {

	// increment current term
	time.Sleep(20 * time.Millisecond)
	
	// Empty BecomeLeaderCH before proceeding
	select {
	case <-rf.BecomeLeaderCH:
	case <-rf.heartbeatCH:
		println("no longer panic, back to follower")
		rf.state = "follower"
		return
	default:
	}
	rf.mu.Lock()
	rf.CurrentTerm = rf.CurrentTerm + 1
	// voteFor itself
	rf.VotedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()
	// send RequestVoteRPC to all other servers, retry until:
	// 1. Receive votes from majority of servers
	// 		* Become leader
	// 		* Send AppendEntries heartbeat to all other servers, aka: change to "leader" and back to main Loop()
	// 2. Receive RPC from valid leader
	//		* Return to "follower" state
	// 3. No-one win election(election timeout elapses)
	// 		* Increment term, start new election

	// send RequestVote to all other nodes, and wait for BecomeLeaderCH
	rf.BroadcastRequestVote()

	select {
	case becomeLeader := <-rf.BecomeLeaderCH:
		// change state to leader
		if becomeLeader {
			rf.state = "leader"
			println(strconv.Itoa(rf.me) + " becomes leader" + " CommitIndex: " + strconv.Itoa(rf.CommitIndex) + " LastApplied: " + strconv.Itoa(rf.LastApplied) + " loglength: " + strconv.Itoa(len(rf.Logs)) + " Term: " + strconv.Itoa(rf.CurrentTerm))
			// When a leader first comes to power, it initializes all
			// NextIndex values to the index just after the last one in its log.
			rf.mu.Lock()
			rf.NextIndex = []int{}
			for i := 0; i < len(rf.peers); i++ {
				rf.NextIndex = append(rf.NextIndex, len(rf.Logs)-1)
			}
			rf.mu.Unlock()

			go rf.BroadcastAppendEntriesRPC()
			return
		}
	case <-time.After(time.Duration(TimeOutConst) * time.Millisecond):
		println(strconv.Itoa(rf.me) + " become leader fail...")
		if rf.state != "leader" {
			rf.state = "follower"
		}
		return
	}
}

func (rf *Raft) LeaderState() {
	// broadcast heatbeat to all other nodes in the cluster
	time.Sleep(10 * time.Millisecond)
	if rf.LastApplied == rf.CommitIndex {
		time.Sleep(25 * time.Millisecond)
	}
	go rf.BroadcastAppendEntriesRPC()
	
}

func ElectionTimeoutConst() int {
	res := rand.Intn(300) + 800
	return res
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// rf.state = "follower"
	rf.BecomeLeaderCH = make(chan bool)
	rf.heartbeatCH = make(chan bool)
	rf.commandCH = make(chan interface{}, 10)
	// rf.VotedFor = -1
	// rf.NextIndex = []int{} // initialize all node's NextIndex

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Loop()
	go rf.FeedStateMachine(applyCh)

	return rf
}
