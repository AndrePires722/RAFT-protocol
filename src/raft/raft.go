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
	"bytes"
	"encoding/gob"
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
	Index int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	applyMu   sync.Mutex
	applyCh chan ApplyMsg
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	
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
	//committing bool
	lastSent int
	
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) getLastTerm() int{
	
	if(len(rf.log)>0){
		return rf.log[len(rf.log)-1].Term
	} 
	return 0
	
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//fmt.Printf("%v saved state: Log:%v Term:%v CI:%v\n",rf.me,rf.log,rf.currentTerm,rf.commitIndex)
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
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.log)
	d.Decode(&rf.commitIndex)
	if(len(rf.log)>0){
		rf.log = rf.log[:rf.commitIndex]
	}
	fmt.Printf("%v Recovered from crash: Log:%v Term:%v CI:%v\n",rf.me,rf.log,rf.currentTerm,rf.commitIndex)
	
}


// used for heartbeat
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
	////fmt.Printf("%v got a ping from %v, with term of %v   @  %v\n",rf.me,args.ID,args.Term,rf.lastPing)

	//rf.candidate = false
	
	reply.Success=true
	
	
	////fmt.Printf("%v  BEFORE :   %v   Term %v, Requestee is %v w/ T:%v\n",rf.me,rf.log,rf.currentTerm,args.ID,args.Term)
	//C1
	if(args.Term>rf.currentTerm){
		//fmt.Printf("%v to %v: Fail C1   ;   term %v to %v\n",args.ID,rf.me,args.Term,rf.currentTerm)
		rf.leader = false
		rf.candidate = false
		rf.currentTerm = args.Term
		rf.persist()
		reply.Success=false
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
	}
	if(args.LeaderCommit<rf.commitIndex){
		reply.Success = false
	}
	//C2
	if(args.PrevLogIndex>len(rf.log) || (args.PrevLogIndex-1>=0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm)){
		//fmt.Printf("%v to %v: Fail C2 : %v vs %v\n",args.ID,rf.me,args.PrevLogIndex,len(rf.log))
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	if(!reply.Success){
		return
	}
	//C3
	if(len(args.Entries)>0){
		rf.log = rf.log[:args.Entries[0].Index])
	}
	conflict:=0
	if args.PrevLogIndex-1>=0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		//Clause 3
		//fmt.Printf("%v to %v: Fail C3\n",args.ID,rf.me)
		rf.log = rf.log[:args.PrevLogIndex-1]
	}else{
		////fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>CHECKING REST OF LOG! PrevLogIndex:%v\n",args.PrevLogIndex)
		for i:=0;args.PrevLogIndex+i<len(rf.log) && i<len(args.Entries);i++{
			////fmt.Printf("CHECKING INDEX %v OF NEW ENTRIES\n",i)
			if(rf.log[args.PrevLogIndex+i].Term < args.Entries[i].Term){
				conflict = i
				//fmt.Printf("%v: ITS A BAD ENTRY! SHRINKING LOG\n",rf.me)
				rf.log = rf.log[:args.PrevLogIndex+i]
				rf.persist()
				break
			}
		}
		
	}
	
	

	
	//C4
	rf.log = append(rf.log,args.Entries[conflict:]...)
	
	//C5
	if(args.LeaderCommit > rf.commitIndex){
		if(args.LeaderCommit < len(rf.log)){
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
	}
	rf.persist()
	//=========================================
	
	
	
	
	
	//fmt.Printf("LEADER COMMIT IS %v WHILE %v's IS %v AND LAST APPLIED IS %v\n",args.LeaderCommit,rf.me,rf.commitIndex,rf.lastApplied)
	
	fmt.Printf("%v  AFTER :   %v   Term %v\n",rf.me,rf.log,rf.currentTerm)
	////fmt.Printf("%v Got a ping!\n",rf.me)
	
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


func (rf *Raft) isUpToDate(lastlogIndexOther,lastlogTermOther int) bool{
	var t int
	if(len(rf.log)==0){
		t=0
	}else{
		t = rf.log[len(rf.log)-1].Term
	}
	lastlogIndexSelf, lastlogTermSelf := len(rf.log), t
	
	//fmt.Printf("isUpToDate Check @%v:  (%v,%v) vs (%v,%v)\n",rf.me,lastlogIndexSelf,lastlogTermSelf,lastlogIndexOther,lastlogTermOther)
	
	if lastlogTermOther != lastlogTermSelf {
		return lastlogTermOther > lastlogTermSelf
	}
	return lastlogIndexOther >= lastlogIndexSelf
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
		fmt.Printf("(%v): %v vs my %v\n",rf.me,args.Term,rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(rf.isUpToDate(args.LastLogIndex,args.LastLogTerm)){
			
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.leader = false
		rf.candidate = false
		return
	}else{
		fmt.Println("NOT UP TO DATE!")
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
	////fmt.Println("sendvote")
	ok := server.Call("Raft.RequestVote", args, reply)
	
	////fmt.Println("sendvote done")
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
	defer rf.mu.Unlock()
	defer rf.persist()
	if(!rf.leader){
		return -1,0,rf.leader
	}
	
	entry := LogMessage{
		Command: command,
		Term:    rf.currentTerm,
		Index: 	 len(rf.log)+1,
	}
	
	rf.log = append(rf.log,entry)
	fmt.Printf("[Start] %v got %v, will commit @ %v\nMy log is now: %v\n",rf.me,command,len(rf.log),rf.log)
	
	
	return len(rf.log), rf.currentTerm, rf.leader
}

func (rf *Raft) WaitCommit(msg interface{},ch chan bool){
	//fmt.Printf("[WaitCommit@%v] Waiting...\n",rf.me)
	c:=1
	for(c<len(rf.peers)/2+1){
		res:= <-ch
		if(!res){
			rf.applyMu.Unlock()
			return
		}
		c++
	}
	//fmt.Printf("[WaitCommit@%v] Done Waiting!\n",rf.me)
	rf.mu.Lock()
	//rf.lastApplied++
	rf.commitIndex++
	rf.commitIndex = min(rf.commitIndex,len(rf.log))
	//fmt.Printf("%v ready to commit. len : %v  about to apply to index %v\n",rf.me,len(rf.log),rf.commitIndex)
	//rf.applyCh <- ApplyMsg{rf.lastApplied,rf.log[rf.lastApplied-1].Command,false,nil}
	//fmt.Printf(">>>>>>>>>>[%v]Safe to Commit   %v @ %v\n",rf.me,rf.log[rf.commitIndex-1].Command,rf.commitIndex)
	rf.persist()
	rf.mu.Unlock()
	rf.applyMu.Unlock()
	
	
}

func CheckForNewCommands(rf *Raft){
	////fmt.Println("[CheckForCMDs] Checking...")
	time.Sleep(100*time.Millisecond)
	rf.mu.Lock()
	defer CheckForNewCommands(rf)
	
	if(rf.commitIndex>rf.lastApplied){
				//rf.applyMu.Lock()
				rf.lastApplied++
				//fmt.Printf("%v LOG: %v\n",rf.me,rf.log)
				fmt.Printf("%v COMMITTED %v   @%v\n",rf.me,rf.log[rf.lastApplied-1],rf.lastApplied)
				rf.applyCh <- ApplyMsg{rf.lastApplied,rf.log[rf.lastApplied-1].Command,false,nil}	
				
				//rf.applyMu.Unlock()
			

	}
	
	
	
	if(!rf.leader){
		rf.mu.Unlock()
		return
	}
	
	if(len(rf.log)>rf.lastSent){
		rf.lastSent++
		rf.mu.Unlock()
		rf.applyMu.Lock()
		rf.mu.Lock()
		////fmt.Printf("[CheckCMDs@%v] : New Item DETECTED!!!\n",rf.me)
		
		ch := make(chan bool)
		go rf.WaitCommit(rf.log[rf.lastSent-1].Command,ch)
		////fmt.Println("WE BACK")
		args := make([]AppendEntriesArgs,len(rf.peers))
			////fmt.Println("WE BACK HERE")			
		
		for i,e := range(rf.peers){
			////fmt.Printf("WE BACK @ %v\n",i)
			if(i!=rf.me){
				////fmt.Printf("WE in @ %v\n",i)
				
				var prevLogTerm int
				if(rf.nextIndex[i]-2>=0&&rf.nextIndex[i]-2<len(rf.log)){
					prevLogTerm = rf.log[rf.nextIndex[i]-2].Term
				}else{
					prevLogTerm = 0
				}
				////fmt.Printf("We REALLY IN @ %v\n",i)
				args[i] = AppendEntriesArgs{rf.currentTerm,rf.me,rf.nextIndex[i]-1,prevLogTerm,rf.log[rf.nextIndex[i]-1:],rf.commitIndex	}
		
				////fmt.Printf("[CheckCMDs@%v] Request made to %v\n",rf.me,i)
				
				
				go func(index int,e2 *labrpc.ClientEnd){
					
					for(true){
						
						if(!rf.leader || rf.dead){
							ch <- false
							return
						}
						
						
						
						x:= rf.nextIndex[index]-1
						if(x<0 || x>len(rf.log)){
							//fmt.Println("UH OH GAMERS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
							return
						}
						//args := AppendEntriesArgs{rf.currentTerm,rf.me,rf.nextIndex[index]-1,prevLogTerm,rf.log[rf.nextIndex[index]-1:],rf.commitIndex	}
						//rf.nextIndex[index]++
						//fmt.Printf("%v Sending to %v: %v\n",rf.me,index,args[index])
						reply := AppendEntriesReply{}
						ok := sendAppendEntries(e2,&args[index],&reply)
						
						if(ok){
							//fmt.Printf("Call from %v to %v went through ok! Response was: %v\n",rf.me,index,reply.Success)
							rf.mu.Lock()
							if(!reply.Success &&reply.Term>rf.currentTerm){
								rf.currentTerm = reply.Term
								rf.leader = false
								rf.candidate = false
								rf.mu.Unlock()
								continue
							}
							rf.mu.Unlock()
							////fmt.Printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>    [  %v  ]\n",rf.nextIndex[index]) 
							if(reply.Success){
								defer func(){ch <- true}()
								rf.mu.Lock()
								rf.nextIndex[index]+=len(args[index].Entries)
								
								rf.mu.Unlock()
								
								
								
								//fmt.Printf(">>%v done with %v, nextIndex is %v\n",rf.me,index,rf.nextIndex[index])
								return
							}else{
								if(rf.nextIndex[index]==1){
									continue
								}
								rf.mu.Lock()
								rf.nextIndex[index]--
								
								var prevLogTerm int
								if(rf.nextIndex[index]-2>=0&&rf.nextIndex[index]-2<len(rf.log)){
									prevLogTerm = rf.log[rf.nextIndex[index]-2].Term
								}else{
									prevLogTerm = 0
								}
								args[index] = AppendEntriesArgs{rf.currentTerm,rf.me,rf.nextIndex[index]-1,prevLogTerm,rf.log[rf.nextIndex[index]-1:],rf.commitIndex	}
								rf.mu.Unlock()
							}
						}else{
							//fmt.Printf("Call to %v Timed out\n",index)
						}
						
					}
				}(i,e)
					
				
			}
		}
		//fmt.Printf("[CheckCMDs@%v] All Requests made\n",rf.me)
		
		
		
		
	}
	rf.mu.Unlock()
}
func (rf *Raft) Kill() {
	// Your code here, if desired.
	
	
	////fmt.Printf("killing %v\n",rf.me)
	////fmt.Println(rf.GetState())
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
	
	////fmt.Println(me)

	// Your initialization code here (2A, 2B, 2C).
	
	rf.currentTerm = 0
	
	rf.votedFor = -1
	
	rf.log = make([]LogMessage,0)
	
	rf.commitIndex = 0
	
	rf.lastApplied = 0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	rf.timeOut = time.Duration(300 + me*800)*time.Millisecond
	rf.applyCh = applyCh2
	rf.lastPing = time.Now()
	
	
	rf.readPersist(persister.ReadRaftState())
	
	
	go CheckForNewCommands(rf)
	go CheckForBeat(rf)

	return rf
}
func (rf *Raft) convertToLeader(){
	rf.lastSent = len(rf.log)
	rf.leader = true
	rf.candidate = false
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))
	for server := range rf.nextIndex {
		rf.nextIndex[server] = len(rf.log) + 1
		rf.matchIndex[server] = 0
	}
	
}
func CheckForBeat(rf *Raft){
	////fmt.Printf("[CheckBeat@%v] Checking...\n",rf.me)
	rf.mu.Lock()
	if(rf.leader||rf.candidate||rf.dead){
		rf.mu.Unlock()
		time.Sleep(100*time.Millisecond)
		go CheckForBeat(rf)
		return
	}
	rf.mu.Unlock()

	time.Sleep(rf.timeOut)
	
	rf.mu.Lock()
	if(time.Now().After(rf.lastPing.Add(rf.timeOut))){
			////fmt.Printf("%v thinks time has expired, time: %v, lastPing: %v, timeout: %v\n",rf.me,time.Now(),rf.lastPing,rf.timeOut)
			//election time
			rf.candidate = true
			rf.mu.Unlock()
			DoElection(rf)
	}else{
			//no need for election
			rf.mu.Unlock()
	}
	
	go CheckForBeat(rf)
}

func max(x,y int) int{
	if(x>y){
		return x
	}
	return y
}
func min(x,y int) int{
	if(x<y){
		return x
	}
	return y
}

func DoElection(rf *Raft){
	rf.mu.Lock()
	if(rf.leader || !rf.candidate ||rf.dead){
		rf.mu.Unlock()
		return
	}
	
	
	
	rf.currentTerm++
	fmt.Printf("ELECTION TIME SAYS %v\n",rf.me)
	var t int
	if(rf.commitIndex==0){
		t = 0
	}else{
		t = rf.log[rf.commitIndex-1].Term
	}
	args := RequestVoteArgs{rf.currentTerm,rf.me,rf.commitIndex,t}
	reply :=  make([]RequestVoteReply,len(rf.peers))
	rf.mu.Unlock()
	////fmt.Printf("Current term is %v\n",rf.currentTerm)
	
	
	votes := 1
	totalVotes := 1
	
	var votesMu sync.Mutex
	
	for i,e := range(rf.peers) {
		if(i==rf.me){continue}
		fmt.Printf("%v IS SENDING REQUEST TO %v\n",rf.me,i)
		go func(index int,e2 *labrpc.ClientEnd){
			ok := sendRequestVote(e2,&args,&reply[index])
			if(ok){
			fmt.Printf("(%v) : Response recieved, ans is %v\n",rf.me,reply[index].VoteGranted)
			if(reply[index].VoteGranted){
				
				votesMu.Lock()
				votes++
				totalVotes++
				votesMu.Unlock()
			}
			}else{
				////fmt.Println("TIMEOUT!")
				votesMu.Lock()
				totalVotes++
				votesMu.Unlock()
			}
			rf.mu.Lock()
			if(reply[index].Term > rf.currentTerm){
				
				rf.currentTerm = reply[index].Term
				rf.persist() 
				rf.leader = false
				rf.candidate = false
				
			}
			rf.mu.Unlock()
		}(i,e)
		
		
	}
	rf.persist()
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
	////fmt.Printf("ELECTION RESULTS: %v\n",votes)
	rf.mu.Lock()
	votesMu.Lock()
	//If you've been elected
	if(rf.candidate&& votes>=(len(rf.peers)/2+1)){
		fmt.Printf("%v IS ELECTED\n",rf.me)
		rf.convertToLeader()
		////fmt.Printf(">>>>%v has started beating\n",rf.me)
		rf.mu.Unlock()
		votesMu.Unlock()
		go Heartbeat(rf)
	}else{
		////fmt.Printf("However, %v is not elected!\n",rf.me)
		rf.mu.Unlock()
		votesMu.Unlock()
	}
	
	////fmt.Println("-----------")
	
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
		fmt.Printf("<<<<%v has stopped beating!\n",rf.me)
		return
	}
	
	args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			ID:     	  rf.me,
			PrevLogIndex: len(rf.log),
			PrevLogTerm:  rf.getLastTerm(),
			Entries:      []LogMessage{},
			LeaderCommit: rf.commitIndex,
		}
		
	reply := make([]AppendEntriesReply,len(rf.peers))
	rf.mu.Unlock()
	
	for i,e := range(rf.peers){
		if(i==rf.me){continue}
		go func(index int,e2 *labrpc.ClientEnd){
			
			sendAppendEntries(e2,&args,&reply[index])
			
			rf.mu.Lock()
			if(reply[index].Term > rf.currentTerm){
				
				rf.currentTerm = reply[index].Term
				rf.persist()
				rf.leader = false
				rf.candidate = false
				
			}
			rf.mu.Unlock()
			
			
		}(i,e)
	}
	
	
	
	defer Heartbeat(rf)
	
	time.Sleep(200*time.Millisecond)
}
