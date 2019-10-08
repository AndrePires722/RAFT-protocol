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
	"sync"
	"labrpc"
	"fmt"
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


type LogMessage struct {
	Command interface{}
	Term int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg
	currentTerm int
	
	votedFor int
	
	log []LogMessage
	
	commitIndex int
	
	lastApplied int
	
	nextIndex []int
	
	matchIndex []int
	
	leader bool
	candidate bool
	dead bool
	
	timeOut time.Duration
	lastPing time.Time
	
	
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.leader
	// Your code here (2A).
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


// used for heartbeat
type AppendEntriesArgs struct {
	Term int
	ID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogMessage
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastPing = time.Now()
	//fmt.Printf("%v got a ping from %v, with term of %v   @  %v\n",rf.me,args.ID,args.Term,rf.lastPing)
	
	
	if(args.Term>rf.currentTerm){
		rf.leader = false
		rf.candidate = false
		rf.currentTerm = args.Term
	}
	
	if(args.LeaderCommit>rf.commitIndex && rf.commitIndex<len(rf.log)){
		
		rf.commitIndex++
		rf.applyCh <- ApplyMsg{rf.commitIndex,rf.log[rf.commitIndex-1].Command,false,nil}
		fmt.Println("COMMITTED")
		
	}
	
	if(args.Entries==nil){
		//fmt.Println("Looks like spam!")
		return
	}
	
	fmt.Printf("%v got an AppendEntry request from %v\n",rf.me,args.ID)
	
	//Clause 1
	if(args.Term < rf.currentTerm){
		fmt.Println("Fail C1")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	
	//Clause 2
	if(args.PrevLogIndex>len(rf.log) || (len(rf.log)>0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm)){
		fmt.Println("Fail C2")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}else if len(rf.log)>0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		//Clause 3
		fmt.Println("Fail C3")
		rf.log = rf.log[:args.PrevLogIndex-1]
	}
	
	
	//Clause 4
	rf.log = append(rf.log,args.Entries...)
	
	//Clause 5
	if(args.LeaderCommit > rf.commitIndex){
		if(args.LeaderCommit < len(rf.log)){
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
	}
	
	reply.Success = true
	reply.Term = rf.currentTerm
	
	fmt.Println(rf.log)
	//fmt.Printf("%v Got a ping!\n",rf.me)
	
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//


type RequestVoteReply struct {
	// Your data here (2A).
	
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	
	
	//fmt.Printf("%v is considering the vote for %v at term %v\n",rf.me,args.CandidateId,args.Term)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if(args.Term > rf.currentTerm){
		rf.currentTerm = args.Term
		rf.leader = false
		rf.candidate = false
	}
	
	if(args.Term < rf.currentTerm){
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogIndex >= rf.commitIndex){
			
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.leader = false
		rf.candidate = false
		rf.lastPing = time.Now()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
func sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("sendvote")
	ok := server.Call("Raft.RequestVote", args, reply)
	//fmt.Println("sendvote done")
	return ok
}

func sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs,reply *AppendEntriesReply) bool {
	ok := server.Call("Raft.AppendEntries",args,reply)
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
	

	rf.mu.Lock()
	if(rf.leader){
		fmt.Printf("%v got a thing and is also a leader!\n",rf.me)
		
		var prevLogTerm int
		if(len(rf.log)>0){
			prevLogTerm = rf.log[len(rf.log)-1].Term
		}else{
			prevLogTerm = 0
		}
		msg := make([]LogMessage,1)
		msg[0] = LogMessage{command,rf.currentTerm}
		args := AppendEntriesArgs{rf.currentTerm,rf.me,len(rf.log),prevLogTerm,msg,rf.commitIndex}
		rf.log = append(rf.log,LogMessage{command,rf.currentTerm})
		rf.commitIndex++
		rf.applyCh <- ApplyMsg{rf.commitIndex,command,false,nil}
		rf.mu.Unlock()
		for i,e := range(rf.peers){
			if(i!=rf.me){
				go func(index int,e2 *labrpc.ClientEnd){
					
					reply := AppendEntriesReply{}
					ok := sendAppendEntries(e2,&args,&reply)
					if(ok){
						fmt.Printf("Call to %v went through ok! Response was: %v\n",index,reply.Success)
						
					}else{
						fmt.Printf("Call to %v Timed out\n",index)
					}
				}(i,e)
			}
		}
	}else{
		rf.mu.Unlock()
		//fmt.Printf("but %v isnt a leader.\n",rf.me)
		return rf.commitIndex,rf.currentTerm,false
	}
	//fmt.Println("Ret")
	return rf.commitIndex,rf.currentTerm,true
	
	

	// Your code here (2B).


	//return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
	
	
	//fmt.Printf("killing %v\n",rf.me)
	//fmt.Println(rf.GetState())
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = false
	rf.candidate = false
	rf.dead = true
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
	persister *Persister, applyCh2 chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh2
	
	fmt.Printf("Initializing %v\n",me)

	// Your initialization code here (2A, 2B, 2C).
	
	rf.currentTerm = 0
	
	rf.votedFor = -1
	
	rf.log = make([]LogMessage,0)
	
	rf.commitIndex = 0
	
	rf.lastApplied = 0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	rf.timeOut = time.Duration(300 + me*800)*time.Millisecond
	
	rf.lastPing = time.Now()
	
	go CheckForBeat(rf)

	return rf
}

func CheckForBeat(rf *Raft){
	
	rf.mu.Lock()
	if(rf.leader||rf.candidate||rf.dead){
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	time.Sleep(rf.timeOut)
	
	rf.mu.Lock()
	if(time.Now().After(rf.lastPing.Add(rf.timeOut))){
			//fmt.Printf("%v thinks time has expired, time: %v, lastPing: %v, timeout: %v\n",rf.me,time.Now(),rf.lastPing,rf.timeOut)
			//election time
			rf.candidate = true
			rf.mu.Unlock()
			DoElection(rf)
	}else{
			//no need for election
			rf.mu.Unlock()
	}
	
	rf.mu.Lock()
	if(!rf.leader){
		defer CheckForBeat(rf)
	}
	rf.mu.Unlock()
	
	
}


func DoElection(rf *Raft){
	rf.mu.Lock()
	if(rf.leader || !rf.candidate ||rf.dead){
		rf.mu.Unlock()
		return
	}
	
	rf.lastPing = time.Now()
	
	rf.currentTerm++
	//fmt.Printf("ELECTION TIME SAYS %v\n",rf.me)
	var prevLogTerm int
	if(len(rf.log)>0){
		prevLogTerm = rf.log[len(rf.log)-1].Term
	}else{
		prevLogTerm = 0
	}
	args := RequestVoteArgs{rf.currentTerm,rf.me,0,prevLogTerm}
	reply :=  make([]RequestVoteReply,len(rf.peers))
	rf.mu.Unlock()
	//fmt.Printf("Current term is %v\n",rf.currentTerm)
	
	
	votes := 1
	totalVotes := 1
	
	var votesMu sync.Mutex
	
	for i,e := range(rf.peers) {
		if(i==rf.me){continue}
		//fmt.Printf("%v IS SENDING REQUEST TO %v\n",rf.me,i)
		go func(index int,e2 *labrpc.ClientEnd){
			ok := sendRequestVote(e2,&args,&reply[index])
			if(ok){
			if(reply[index].VoteGranted){
				//fmt.Printf("(%v) : Response recieved, ans is %v\n",rf.me,reply[index].VoteGranted)
				votesMu.Lock()
				votes++
				totalVotes++
				votesMu.Unlock()
			}
			}else{
				//fmt.Println("TIMEOUT!")
				votesMu.Lock()
				totalVotes++
				votesMu.Unlock()
			}
			rf.mu.Lock()
			if(reply[index].Term > rf.currentTerm){
				
				rf.currentTerm = reply[index].Term
				
				rf.leader = false
				rf.candidate = false
				
			}
			rf.mu.Unlock()
		}(i,e)
		
		
	}
	//rf.mu.Unlock()
	for(true){
		votesMu.Lock()
		if(votes<(len(rf.peers)/2+1) && totalVotes!=len(rf.peers)){
				votesMu.Unlock()
				continue
		}else{
				votesMu.Unlock()
				break
		}
		
	}
	//fmt.Printf("ELECTION RESULTS: %v\n",votes)
	rf.mu.Lock()
	votesMu.Lock()
	//If you've been elected
	if(rf.candidate&& votes>=(len(rf.peers)/2+1)){
		//fmt.Printf("%v IS ELECTED\n",rf.me)
		rf.leader = true
		//fmt.Printf(">>>>%v has started beating\n",rf.me)
		rf.mu.Unlock()
		votesMu.Unlock()
		go Heartbeat(rf)
	}else{
		//fmt.Printf("However, %v is not elected!\n",rf.me)
		rf.mu.Unlock()
		votesMu.Unlock()
	}
	
	//fmt.Println("-----------")
	
	for(true){
		votesMu.Lock()
		if(totalVotes!=len(rf.peers)){
				votesMu.Unlock()
				continue
		}else{
				votesMu.Unlock()
				break
		}
		
	}
	//rf.mu.Unlock()
	
	
}

func Heartbeat(rf *Raft){
	rf.mu.Lock()
	if(!rf.leader || rf.dead){
		rf.mu.Unlock()
		//fmt.Printf("<<<<%v has stopped beating!\n",rf.me)
		return
	}
	var prevLogTerm int
	if(len(rf.log)>0){
		prevLogTerm = rf.log[len(rf.log)-1].Term
	}else{
		prevLogTerm = 0
	}
	args := AppendEntriesArgs{rf.currentTerm,rf.me,len(rf.log),prevLogTerm,nil,rf.commitIndex}
	reply := make([]AppendEntriesReply,len(rf.peers))
	rf.mu.Unlock()
	
	for i,e := range(rf.peers){
		if(i==rf.me){continue}
		go func(index int,e2 *labrpc.ClientEnd){
			
			sendAppendEntries(e2,&args,&reply[index])
			
			rf.mu.Lock()
			if(reply[index].Term > rf.currentTerm){
				
				rf.currentTerm = reply[index].Term
				
				rf.leader = false
				rf.candidate = false
				
			}
			rf.mu.Unlock()
			
			
		}(i,e)
	}
	
	
	
	defer Heartbeat(rf)
	
	time.Sleep(100*time.Millisecond)
}
