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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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

const (
	leader = iota
	candidate
	follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	// should be stored in persister and recover it when restart
	currentTerm int
	voteFor     int
	logs        []*Entry

	// volatile state
	isLeader bool
	// role        int
	commitIndex int
	lastApplied int

	// for leader
	matchIndex []int
	nextIndex  []int

	// other
	// resetTickCh     chan struct{}
	receiveAppendCh chan struct{}
}

type Entry struct {
	Term    int
	Command string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// todo: use persister
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
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
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	logs         []*Entry
	leaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}
	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}

	// if success
	go func() {
		// rf.resetTickCh <- struct{}{}
		rf.receiveAppendCh <- struct{}{}

	}()
	// if term > currentterm, update
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate    int
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	reply.Term = currentTerm
	reply.VoteGranted = false
	if args.Term < currentTerm {
		return
	}
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	// todo: use candidateId
	if rf.voteFor == -1 || // todo: wrong
		args.LastLogTerm > lastLogTerm || // if candidate's log is up-to-date
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.VoteGranted = true
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}
	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).

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
	// rf.resetTickCh = make(chan struct{})
	rf.receiveAppendCh = make(chan struct{})

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]*Entry, 0)

	rf.isLeader = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.logs)
	}

	go electionLoop(rf)

	go heartbeatLoop(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func heartbeatLoop(rf *Raft) {
	// send heartbeat periodly
	tick := time.NewTicker(150 * time.Millisecond)
	for {
		select {
		case <-tick.C:
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(index int) {
					rf.sendAppendEntries(index, nil, nil)
				}(i)
			}
		}

	}

}

func electionLoop(rf *Raft) {
	ticker := getNewTicker()
	for {
		// if not leader and timeout of ticker, do request vote
		// 1. timeout should be greater than multi times of heartbeat period, think about 150 * 2 = 300
		// 2. election timeout: (1) no append request  (2) during election
		select {
		case <-rf.receiveAppendCh: // receive append
			ticker = getNewTicker() // todo: confirm whether it's problem if don't release old ticker
			continue
		case <-ticker.C:
			rf.mu.Lock()
			if rf.isLeader { // leader don't need to start election
				ticker = getNewTicker()
				rf.mu.Unlock()
				continue
			}
			rf.currentTerm++

			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIndex].Term
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				Candidate:    rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			rf.mu.Unlock()
			voteChan := make(chan bool)
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(index int) {
					// if ok := rf.sendRequestVote(i, args, reply); ok {
					// votes++
					// }
					reply := &RequestVoteReply{}
					if ok := rf.sendRequestVote(index, args, reply); !ok {
						voteChan <- false
					} else {
						voteChan <- reply.VoteGranted
					}

				}(i)
			}

			// timeout = rand.Intn(150) + 300
			// tick = time.NewTicker(time.Duration(timeout) * time.Millisecond)
			ticker = getNewTicker() // todo: maybe placed more upward

			votes := 1
			total := 1
			for {
				select {
				case <-rf.receiveAppendCh:
					break
				case <-ticker.C: // election timeout
					break
				case isGrant := <-voteChan:
					if isGrant {
						votes++
						if votes > len(rf.peers)/2 { // votes num >= majority, become leader
							rf.mu.Lock()
							rf.isLeader = true
							for i := 0; i < len(rf.matchIndex); i++ {
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = len(rf.logs)
							}

							rf.mu.Unlock()
						}
						break
					}
					total++
					if total == len(rf.peers) { // all request done
						break
					}
				}

			}
			ticker = getNewTicker()

		}
	}
}

// election ticker
func getNewTicker() *time.Ticker {
	rand := rand.New(rand.NewSource(time.Now().Unix()))
	timeout := rand.Intn(150) + 300
	tick := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	return tick
}
