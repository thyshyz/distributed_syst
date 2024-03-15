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

const HRAETBEAT_INTERVAL time.Duration = 150 * time.Millisecond

const (
	leader int = iota
	follower
	condidator
)

type entry struct {
	Term int
	Task interface{}
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
	currentTerm     int
	voteFor         int
	state           int
	electionTimeOut Timer
	logs            []entry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
	applierCh       chan struct{}
}

// type Timer to record the timeout
type Timer struct {
	timer *time.Ticker
}

// set the election time out of the follwer and condidate
func (t *Timer) ResetTimeOut() {
	randomTime := time.Duration(250+rand.Intn(150)) * time.Millisecond
	t.timer.Reset(randomTime)
}

// set heatbeat interval(should < election timeout) for leader
func (t *Timer) ResetHeartBeat() {
	t.timer.Reset(HRAETBEAT_INTERVAL)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == leader)
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = follower
	}
	isNewest := args.LastLogTerm > rf.LastLogTerm() ||
		(args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	if (rf.voteFor != -1 && rf.voteFor != args.CandidateId) || !isNewest {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimeOut.ResetTimeOut()
	rf.voteFor = args.CandidateId
	//fmt.Printf("%d vote for %d in term %d.\n", rf.me, rf.voteFor, rf.currentTerm)
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// return the last log index(not +1) of the server,it does not have locks,so remember to add lock before using it
func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) LastLogTerm() int {
	return rf.logs[rf.lastLogIndex()].Term
}

// leader appendentreis to one of rf.peers
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// fmt.Printf("%d refuse heartbeat from %d in term %d,cause the currentTerm is %d\n", rf.me, args.LeaderId,
		// 	args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.voteFor = -1
	}
	rf.state = follower //why not put here?
	rf.electionTimeOut.ResetTimeOut()

	//reply false if log at prevLogIndex does't match
	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if args.PrevLogIndex > rf.lastLogIndex() {
			reply.ConflictIndex = rf.lastLogIndex() + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			idx := args.PrevLogIndex
			for idx > 0 && rf.logs[idx].Term == reply.ConflictTerm {
				idx--
			}
			reply.ConflictIndex = idx + 1
		}
		return
	}

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx > rf.lastLogIndex() || rf.logs[idx].Term != entry.Term {
			rf.logs = rf.logs[0:idx]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			//fmt.Printf("After AppendEntry, follwer %d change to %v\n", rf.me, rf.logs)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		go func() {
			//fmt.Printf("Receive applierCh\n")
			rf.applierCh <- struct{}{}
		}()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	//fmt.Printf("%d receive heartbeat from %d in term %d\n", rf.me, args.LeaderId, rf.currentTerm)
}

// start a new round of election
func (rf *Raft) startElection() {
	//muElection := sync.Mutex{}
	rf.mu.Lock()
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.state = condidator
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.logs[rf.lastLogIndex()].Term,
	}
	//rf.electionTimeOut.ResetTimeOut()
	rf.mu.Unlock()
	finished := 1
	count := 1
	//condi for await
	condi := sync.NewCond(&rf.mu)

	//ask for other raft server for vote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				//muElection.Lock()
				rf.mu.Lock()
				finished++
				condi.Broadcast()
				//muElection.Unlock()
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			//after send rpc finished, the rf.currentterm has changed,we simply drop the reply and return
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}

			if reply.Term < rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.voteFor = -1
				//muElection.Lock()
				finished++ //every time finished or count change,call condi.broadcast()
				condi.Broadcast()
				//muElection.Unlock()
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				count++
				condi.Broadcast()
			}
			finished++
			condi.Broadcast()
			rf.mu.Unlock()
		}(i)
	}

	//count the vote number the condidate have
	rf.mu.Lock()
	for count <= len(rf.peers)/2 && finished != len(rf.peers) {
		condi.Wait()
	}
	if rf.state != condidator {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	if count > len(rf.peers)/2 {
		rf.mu.Lock()
		rf.state = leader
		rf.initLeader()
		//fmt.Printf("%d become leader in election in term %d.\n", rf.me, rf.currentTerm)
		rf.electionTimeOut.ResetHeartBeat()
		rf.mu.Unlock()
		go rf.heartBeat(false)
	} else {
		rf.mu.Lock()
		//fmt.Printf("%d didn't become leader in election in term %d.\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	}
}

// should in lock
func (rf *Raft) initLeader() {
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
}

// the leader peridiocally send appendentries with empty log to all other nodes
func (rf *Raft) heartBeat(withCommand bool) {
	// rf.mu.Lock()
	// args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	// rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// is it safe to prepare args in for loop?
		rf.mu.Lock()
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		if rf.nextIndex[i] <= rf.lastLogIndex() {
			args.Entries = rf.logs[rf.nextIndex[i]:]
			//fmt.Printf("in args,the args.entries is %v\n", args.Entries)
		}
		rf.mu.Unlock()

		if rf.nextIndex[i] <= rf.lastLogIndex() || !withCommand {
			go func(i int) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//after send rpc finished, the rf.currentterm has changed,we simply drop the reply and return
				if args.Term != rf.currentTerm || rf.state != leader {
					return
				}

				// fmt.Printf("leader %d send heartbeat to %d success!\n", rf.me, i)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					//fmt.Printf("leader %d change to follower in term %d\n", rf.me, rf.currentTerm)
					rf.voteFor = -1
					rf.electionTimeOut.ResetTimeOut()
					return
				}

				// successfully append entry
				if reply.Success {
					// the first reply must be rf.nextIndex[i]+len(args.Entries) = rf.lastLogIndex()+1
					// if not, this means the go routine receives a repeat reply
					// we simply drop it
					//fmt.Printf("nextIndex in index %d is %d,entries are %v\n", i, rf.nextIndex[i], args.Entries)
					if rf.nextIndex[i]+len(args.Entries) > rf.lastLogIndex()+1 {
						//fmt.Printf("%d receive repeat appendEntry reply!\n", i)
						return
					}
					rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					rf.decideCommit()
				} else {
					index := -1
					found := false
					for i, entry := range rf.logs {
						if entry.Term == reply.ConflictTerm {
							index = i
							found = true
						} else if found {
							break
						}
					}
					if found {
						rf.nextIndex[i] = index + 1
					} else {
						rf.nextIndex[i] = reply.ConflictIndex
					}
				}
			}(i)
		}
	}
}

// don't add extra lock,we will call this func with lock
func (rf *Raft) decideCommit() {
	N := rf.lastLogIndex()
	for N > rf.commitIndex {
		if rf.logs[N].Term != rf.currentTerm {
			N--
			continue
		}
		count := 1
		for _, matchindex := range rf.matchIndex {
			if matchindex >= N {
				count++
			}
		}
		if count <= len(rf.peers)/2 {
			N--
			continue
		}
		//oldCommitIndex := rf.commitIndex
		rf.commitIndex = N
		//fmt.Printf("leader %d commit entry %v\n", rf.me, rf.logs[oldCommitIndex+1:N+1])
		go func() {
			//fmt.Printf("Receive applierCh   2\n")
			rf.applierCh <- struct{}{}
		}()
		return
	}
}

func (rf *Raft) applier() {
	//fmt.Println("applier go routine start!")
	for !rf.killed() {
		select {
		case <-rf.applierCh:
			rf.mu.Lock()
			lastApplied := rf.lastApplied
			rf.lastApplied = rf.commitIndex
			entries := rf.logs[lastApplied+1 : rf.commitIndex+1]
			//fmt.Printf("entries %v will apply to %d\n", entries, rf.me)
			rf.mu.Unlock()
			for i, entry := range entries {
				//fmt.Printf("entry %v in index %d applied to %d\n", entry, lastApplied+i+1, rf.me)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Task,
					CommandIndex: lastApplied + i + 1,
				}
			}
		}
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

// send AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	//defer rf.mu.Unlock()
	if rf.state != leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index, term, isLeader = len(rf.logs), rf.currentTerm, true
	rf.logs = append(rf.logs, entry{
		Term: rf.currentTerm,
		Task: command,
	})
	//fmt.Printf("client send command, leader %d logs change to %v\n", rf.me, rf.logs)
	rf.mu.Unlock()
	go rf.heartBeat(true)
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

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.electionTimeOut.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case follower:
				//fmt.Printf("follwer %d time out in term %d,become condidate\n", rf.me, rf.currentTerm)
				rf.state = condidator
				fallthrough
			case condidator:
				//fmt.Printf("condidate %d time out in term %d,start election again\n", rf.me, rf.currentTerm)
				// rf.voteFor = rf.me
				rf.electionTimeOut.ResetTimeOut()
				rf.mu.Unlock()
				rf.startElection()
			case leader:
				//fmt.Printf("leader %d time out in term %d,start heartbeat\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.heartBeat(false)
			}
		}

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
	// 2A
	rf.state = follower
	rf.voteFor = -1
	rf.electionTimeOut = Timer{time.NewTicker(time.Duration(time.Duration(250+rand.Intn(200)) * time.Millisecond))}

	//2B
	rf.logs = []entry{}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = append(rf.logs, entry{0, nil})
	rf.applyCh = applyCh
	rf.applierCh = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
