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

// import "strconv"

// import "bytes"
// import "encoding/gob"

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
	currentTerm int
	votedFor    int
	log         []*Log

	commitIndex int
	lastApplied int

	state string

	nextIndex  []int
	matchIndex []int

	heartbeatCH chan bool

	BecomeLeaderCH chan bool // used in candidiate state
	voteCount      int       // used in candidate state, reset everytime when became a candidate

}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here.
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
	if rf.currentTerm < args.TERM || (rf.currentTerm == args.TERM && (rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID)) {
		rf.mu.Lock()
		rf.votedFor = args.CANDIDATEID
		rf.currentTerm = args.TERM
		rf.state = "follower"
		rf.mu.Unlock()
		reply.VOTEGRANTED = true
		return
	} else {
		reply.VOTEGRANTED = false
		return
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
	for {
		if ok {
			if reply.VOTEGRANTED {
				rf.mu.Lock()
				rf.voteCount = rf.voteCount + 1
				rf.mu.Unlock()
				if rf.state == "candidate" && rf.voteCount > len(rf.peers)/2 {
					rf.BecomeLeaderCH <- true
					rf.state = "leader"
				}
			}
			break
		}
		time.Sleep(20 * time.Millisecond) // try to be gentle
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return ok
}

// send RequestVote to all other nodes in the cluster
func (rf *Raft) BroadcastRequestVote() {

	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}

	args.CANDIDATEID = rf.me
	args.TERM = rf.currentTerm // current candidate's term.
	// args.LASTLOGIDX = rf.log[len(rf.log)-1].Index
	// args.LASTLOGTERM = rf.log[len(rf.log)-1].Term
	reply.TERM = rf.currentTerm

	gochan := make(chan int, len(rf.peers)-1)

	// send to all other nodes in parallel
	for k := 0; k < len(rf.peers); k++ {
		if k != rf.me { // exclude self
			gochan <- k
			go func() {
				temp := <-gochan
				rf.sendRequestVote(temp, *args, reply)
			}()
		}
	}
}

// used by leader of the cluster

type AppendEntries struct {
	TERM     int
	LEADERID int
}

type AppendEntriesReply struct {
	TERM   int
	ACCEPT bool
}

func (rf *Raft) AppendEntriesRPC(args AppendEntries, reply *AppendEntriesReply) {

	// AppendEntriesRPC implementation
	// check this so-called leader has up-to-date term
	if args.TERM < rf.currentTerm {
		reply.TERM = rf.currentTerm
		reply.ACCEPT = false
		return
	}
	// if the current listener is in "candidate" state, if so, step down to "follower"
	if rf.state == "candidate" {
		rf.state = "follower"
	}
	// step down if a leader hear from a new leader
	if rf.state == "leader" && rf.currentTerm < args.TERM {
		rf.state = "follower"
	}
	// hear from heartbeat

	rf.heartbeatCH <- true
	rf.currentTerm = args.TERM
	reply.ACCEPT = true
	return
}

func (rf *Raft) sendAppendEntriesRPC(server int, args AppendEntries, reply *AppendEntriesReply) bool {
	// Calling peer node indefinitely until the other side has response (for loop).
	// As long as the other side has response, check if it has accepted as a leader,
	// if not, check if leader's term is up-to-date, if not, step down to follower
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	for {
		if ok {
			if !reply.ACCEPT && rf.currentTerm < reply.TERM {
				rf.state = "follower"
				rf.BecomeLeaderCH <- false
				break
			}
		}
		time.Sleep(20 * time.Millisecond) // try to be gentle
		ok = rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	}

	// if candidate find its term out of date, revert to "follower" state
	if reply.TERM > rf.currentTerm {
		rf.state = "follower"
	}
	return ok
}

// send AppendEntriesRPC to all other nodes in the cluster
func (rf *Raft) BroadcastAppendEntriesRPC() {
	args := &AppendEntries{}
	reply := &AppendEntriesReply{}
	args.TERM = rf.currentTerm
	args.LEADERID = rf.me
	gochan := make(chan int, len(rf.peers)-1)
	for k := 0; k < len(rf.peers); k++ {
		if k != rf.me {
			gochan <- k
			go func() {
				temp := <-gochan
				rf.sendAppendEntriesRPC(temp, *args, reply)
			}()
		}
	}
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
	index := -1
	term := -1
	isLeader := true

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
				rf.state = "candidate"
			}
		} else if rf.state == "candidate" {
			// DO CANDIDIATE STUFF
			rf.CandidateState(TimeOutConst)
		} else {
			// DO LEADER STUFF
			// * send heartbeats
			rf.LeaderState()
		}
	}
}

func (rf *Raft) CandidateState(TimeOutConst int) {

	// increment current term
	rf.currentTerm = rf.currentTerm + 1
	// voteFor itself
	rf.votedFor = rf.me
	rf.voteCount = 1
	// Empty BecomeLeaderCH before proceeding
	select {
	case <-rf.BecomeLeaderCH:
		// println("Empty BecomeLeaderCH")
	default:
		// println("BecomeLeaderCH is empty, carry on.")
	}
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
			return
		}
	case <-time.After(time.Duration(TimeOutConst) * time.Millisecond):
		return
	}
}

func (rf *Raft) LeaderState() {
	// broadcast heatbeat to all other nodes in the cluster
	time.Sleep(20 * time.Millisecond)
	rf.BroadcastAppendEntriesRPC()
}

func ElectionTimeoutConst() int {
	res := rand.Intn(400) + 800
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
	rf.state = "follower"
	rf.BecomeLeaderCH = make(chan bool)
	rf.heartbeatCH = make(chan bool)
	rf.votedFor = -1

	// Your initialization code here.
	go rf.Loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
