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
	"log"
	"math/rand"
	"sort"
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
	// voteFor will be set to server id when: 1. receive RequestVote and args is ok
	// voteFor will be set to -1 when: 1. receive AppendEntries it's term > currentTerm  2. send rpc and receive reply's term > currentTerm
	voteFor int
	// voteTerm int
	logs []*Entry

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
	receiveCh chan struct{}
	// needAppendCh chan struct{}
}

type Entry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) updateReceiveCh() {
	rf.receiveCh <- struct{}{}
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
	// defer log.Printf("[%+v] receive AppendEntries from [%+v], args: %+v, reply: %+v", rf.me, args.LeaderId, args, reply)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Success = false
		log.Printf("server[%+v] deny AppendEntries from [%+v], args: %+v, reply: %+v, raft: %+v", rf.me, args.LeaderId, args, reply, rf)
		rf.mu.Unlock()
		return
	}
	// rf.mu.Lock()
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Success = false
	} else if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	}
	if reply.Success == false {
		go rf.updateReceiveCh()
		rf.mu.Unlock()
		return
	}

	// if reply.Success == true {
	if args.PrevLogIndex >= -1 { // at least one entry
		curLogIndex := args.PrevLogIndex + 1
		if curLogIndex == 0 {
			rf.logs = make([]*Entry, 0)
		} else {
			rf.logs = rf.logs[0:curLogIndex]
		}
		for _, entry := range args.logs {
			rf.logs = append(rf.logs, entry)
			// rf.logs = append(rf.logs, entry)
		}

	}
	// }
	rf.commitIndex = args.leaderCommit
	if len(rf.logs)-1 < args.leaderCommit {
		rf.commitIndex = len(rf.logs) - 1
	}

	if args.Term > rf.currentTerm {
		log.Printf("server[%v] update term from [%v] to [%v]", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term // update term and voteFor
		rf.voteFor = -1            // receive appendEntries args's term > currentTerm, set voteFor = -1
	}
	rf.isLeader = false
	// rf.voteFor = args.LeaderId
	// for _, entry := range args.logs {
	// rf.logs = append(rf.logs, entry)
	// }
	if len(args.logs) > 0 {
		log.Printf("server[%+v] receive AppendEntries from [%+v], args: %+v, reply: %+v", rf.me, args.LeaderId, args, reply)
	}
	rf.mu.Unlock()

	// }

	// if success
	go rf.updateReceiveCh()
	// go func() {
	// // rf.resetTickCh <- struct{}{}
	// rf.receiveCh <- struct{}{}

	// }()
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
	// defer log.Printf("[%v] receive RequestVote from [%+v], raft: %+v, args: %+v, reply: %+v", rf.me, args.Candidate, rf, args, reply)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// currentTerm := rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	// todo: use candidateId
	voteGranted := false
	if args.Term > rf.currentTerm {
		voteGranted = true
	} else if rf.voteFor == -1 && // not vote in currentterm
		(args.LastLogTerm > lastLogTerm || // if candidate's log is up-to-date
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		voteGranted = true
		// if args.Term > rf.currentTerm {
		// }
	}
	if voteGranted {
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.voteFor = args.Candidate
		rf.isLeader = false
		go rf.updateReceiveCh()
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return index, term, false
	}

	// is leader, then:
	// write log
	entry := &Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, entry) // only write one command for once?
	index = len(rf.logs) - 1
	term = rf.currentTerm
	log.Printf("Start() return: index: %v, term: %v, logs: %+v", index, term, rf.logs)

	// sendAppendEntries to other servers
	// prevLogIndex := len(rf.logs) - 2
	// prevLogTerm := 0
	// if prevLogIndex >= 0 {
	// prevLogTerm = rf.logs[prevLogIndex].Term
	// }
	// args := &AppendEntriesArgs{
	// Term:         rf.currentTerm,
	// LeaderId:     rf.me,
	// PrevLogIndex: prevLogIndex,
	// PrevLogTerm:  prevLogTerm,
	// logs:         []*Entry{entry},
	// leaderCommit: rf.commitIndex,
	// }
	rf.mu.Unlock()
	// go func() {
	// rf.needAppendCh <- struct{}{}
	// }()

	// appendReplyCh := make(chan *AppendEntriesReply)
	// for i, _ := range rf.peers {
	// go func(index int) {
	// reply := &AppendEntriesReply{}
	// ok := rf.sendAppendEntries(index, args, reply)
	// appendReplyCh <- reply
	// }(i)
	// }
	// isAppendSucc := make(chan bool)
	// go func() {

	// }()

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
	rf.receiveCh = make(chan struct{})
	// rf.needAppendCh = make(chan struct{})

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]*Entry, 0)

	rf.isLeader = false
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = len(rf.logs)
	}

	go electionLoop(rf)

	// go heartbeatLoop(rf)

	go sendAppendLoop(rf)

	go commitLogLoop(rf, applyCh) // now

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
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				break
			}
			prevLogIndex := len(rf.logs) - 2
			prevLogTerm := 0
			if prevLogIndex >= 0 {
				prevLogTerm = rf.logs[prevLogIndex].Term
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				logs:         []*Entry{},
				leaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(index int) {
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(index, args, reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.isLeader = false
						rf.voteFor = -1

					}
					// log.Printf("server[%+v] send heartbeat to [%+v], args: %+v, reply: %+v", rf.me, index, args, reply)
					rf.mu.Unlock()
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
		case <-rf.receiveCh: // receive append
			ticker = getNewTicker() // todo: confirm whether it's problem if don't release old ticker
			continue
		case <-ticker.C:
			rf.mu.Lock()
			if rf.isLeader { // leader don't need to start election
				ticker = getNewTicker()
				rf.mu.Unlock()
				break
			}
			rf.currentTerm++
			rf.voteFor = rf.me
			log.Printf("server[%v] receive request timeout and start election, current raft: %+v", rf.me, rf)

			lastLogIndex := len(rf.logs) - 1
			lastLogTerm := 0
			if lastLogIndex >= 0 {
				lastLogTerm = rf.logs[lastLogIndex].Term
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				Candidate:    rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			rf.mu.Unlock()
			// voteChan := make(chan bool)
			voteChan := make(chan *RequestVoteReply)
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(index int) {
					// if ok := rf.sendRequestVote(i, args, reply); ok {
					// votes++
					// }
					reply := &RequestVoteReply{}
					// if ok := rf.sendRequestVote(index, args, reply); !ok {
					// voteChan <- false
					// } else {
					// voteChan <- reply.VoteGranted
					// }
					rf.sendRequestVote(index, args, reply)
					voteChan <- reply
					log.Printf("server[%+v] send sendRequestVote to [%+v], args: %+v, reply: %+v", rf.me, index, args, reply)

				}(i)
			}

			finalVote := make(chan bool)
			go func() { // count votes and update
				votes := 1
				total := 1
				electionOver := false
				electionSuccess := false
				for {
					select {
					case reply := <-voteChan:
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.voteFor = -1 // sendRequestVote reply term > current term, set voteFor = -1
							electionOver = true
							rf.mu.Unlock()
							break
						}
						if reply.VoteGranted {
							votes++
							if votes > len(rf.peers)/2 {
								// log.Printf("server[%v] become leader", rf.me)
								electionOver = true
								electionSuccess = true
							}
						}
						rf.mu.Unlock()
						total++
					}
					if total == len(rf.peers) || electionOver {
						break
					}

				}
				// close(voteChan)
				finalVote <- electionSuccess

			}()

			// timeout = rand.Intn(150) + 300
			// tick = time.NewTicker(time.Duration(timeout) * time.Millisecond)
			ticker = getNewTicker() // todo: maybe placed more upward

			// votes := 1
			// total := 1
			// electionOver := false
			// for { // todo: replace this loop
			select {
			case <-rf.receiveCh:
				// electionOver = true
				break
			case <-ticker.C: // election timeout
				// electionOver = true
				break
			case succ := <-finalVote:
				if succ {
					rf.mu.Lock()
					rf.isLeader = true
					for i := 0; i < len(rf.matchIndex); i++ {
						rf.matchIndex[i] = -1
						rf.nextIndex[i] = len(rf.logs)
					}
					log.Printf("server[%+v] become leader, rf: %+v", rf.me, rf)
					rf.mu.Unlock()
				}
				break
			}

			// case reply := <-voteChan:

			// if reply.VoteGranted {
			// votes++
			// if votes > len(rf.peers)/2 { // votes num >= majority, become leader
			// log.Printf("server [%+v] become leader", rf.me)
			// rf.mu.Lock()
			// rf.isLeader = true
			// for i := 0; i < len(rf.matchIndex); i++ {
			// rf.matchIndex[i] = 0
			// rf.nextIndex[i] = len(rf.logs)
			// }

			// rf.mu.Unlock()
			// elecitonOver = true
			// }
			// break
			// }
			// total++
			// if total == len(rf.peers) { // all request done
			// elecitonOver = true
			// break
			// }
			// }
			// if electionOver {
			// close(voteChan)
			// break
			// }

			// }
			ticker = getNewTicker()

		}
	}
}

// election ticker
func getNewTicker() *time.Ticker {
	rand := rand.New(rand.NewSource(time.Now().Unix()))
	timeout := rand.Intn(200) + 300
	tick := time.NewTicker(time.Duration(timeout) * time.Millisecond)
	return tick
}

func sendAppendLoop(rf *Raft) { // should merge the heartbeatloop
	tick := time.NewTicker(150 * time.Millisecond)

	for {
		select {
		case <-tick.C:
			// case <-rf.needAppendCh:

			// if nextIndex >= len(rf.logs) {
			// break
			// }
			// for each server, if nextIndex < len(logs), send appendEntries
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				break
			}
			for i, _ := range rf.peers {
				if !rf.isLeader {
					break
				}
				if i == rf.me {
					continue
				}

				nextIndex := rf.nextIndex[i]
				// if nextIndex >= len(rf.logs) { // just send heartbeat
				// continue
				// }
				prevTerm := 0
				if nextIndex > 0 {
					prevTerm = rf.logs[nextIndex-1].Term
				}
				logs := make([]*Entry, 0)
				// if nextIndex >= rf.logs, then len of sent logs is 0, equals to heartbeat
				for k := nextIndex; k < len(rf.logs); k++ {
					logs = append(logs, rf.logs[k])
				}

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  prevTerm,
					logs:         logs,
					leaderCommit: rf.commitIndex,
				}

				go func(index int) {
					reply := &AppendEntriesReply{}

					rf.sendAppendEntries(index, args, reply)
					rf.mu.Lock()
					if len(args.logs) != 0 {
						log.Printf("leader[%v] sendAppendEntries to server[%v], args: %+v, reply: %+v, raft: %v", rf.me, index, args, reply, rf)
					}
					if !reply.Success {
						if reply.Term > rf.currentTerm { // not leader any more
							rf.currentTerm = reply.Term
							rf.isLeader = false
							rf.voteFor = -1
							log.Printf("[%v] not leader anymore", rf.me)

						} else { // still leader
							if args.PrevLogIndex >= 0 {
								// nextIndex - 1 if failed
								rf.nextIndex[index] = args.PrevLogIndex
								log.Printf("leader[%v] decrease nextIndex[%v] to %v", rf.me, index, args.PrevLogIndex)
							}
						}

						rf.mu.Unlock()
						return
					}
					// update nextIndex and matchIndex
					rf.nextIndex[index] = args.PrevLogIndex + 1 + len(args.logs)
					rf.matchIndex[index] = args.PrevLogIndex + len(args.logs)
					rf.mu.Unlock()

				}(i)

			}
			rf.mu.Unlock()

		}

	}

}

func commitLogLoop(rf *Raft, applyCh chan ApplyMsg) { // now
	tick := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-tick.C:
			// If there exists an N such that N > commitIndex,
			// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				break
			}
			// easy way: sort and choose index in medium
			tmpIndex := make([]int, len(rf.matchIndex))
			copy(tmpIndex, rf.matchIndex)
			sort.Ints(tmpIndex)
			medMatchIndex := tmpIndex[len(tmpIndex)/2]
			if medMatchIndex > rf.commitIndex {
				log.Printf("[%v] get new commit index: %v", rf.me, medMatchIndex)
				oldCommitIndex := rf.commitIndex
				rf.commitIndex = medMatchIndex
				// go func() {
				for i := oldCommitIndex + 1; i <= medMatchIndex; i++ {
					msg := ApplyMsg{
						Index:   i,
						Command: rf.logs[i].Command,
					}
					applyCh <- msg
					log.Printf("apply msg: %v", msg)
				}

				// }()

			}
			rf.mu.Unlock()

		}
	}

}
