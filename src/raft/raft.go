package raft

import (
	log "../debug"
	"../labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (indexStart, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//type int32 int
type ElectionStatusType int

const (
	Follower                                     = 0
	Candidate                                    = 1
	Leader                                       = 2
	ElectionTimeoutSectionStart    int64         = 150
	ElectionTimeoutSectionDuration time.Duration = 150
	VotedForNone                                 = -1
	LogIndexOffset                               = 1
)

var StatusTypeName = []string{"Follower", "Candidate", "Leader"}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                 sync.Mutex // Lock to protect shared access to this peer's state
	mutexAppendEntries sync.Mutex

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's indexStart into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persister statue
	currentTerm int32 // 服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	curStatus   int32
	votedFor    int32        //	当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	log         []LogEntries //日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）

	// volatility statue
	commitIndex int32
	lastApplied int32
	// leader volatility statue
	nextIndex  []int //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []int //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	electionStatus ElectionStatusType

	// definitions of chan
	hasVotedChan         chan struct{}
	voteReceiveChan      chan RequestVoteArgs
	electionDoneChan     chan struct{}
	leaveCurElectionChan chan struct{}
	voteGrantedChan      chan int32
	appendSucceedChan    chan PeerIndex
	requestNewerTermChan chan struct{}
	curLeaderAppendChan  chan struct{}
	replyNewerTermChan   chan struct{}
	leaveFollowerChan    chan struct{}
	leaveLeaderChan      chan struct{}
	applyCh              chan ApplyMsg
}

type PeerIndex struct {
	peerId     int
	indexStart int
	nextIndex  int
}
type LogEntries struct {
	LogTerm int32
	Cmd     interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = int(atomic.LoadInt32(&rf.currentTerm))
	isleader = atomic.LoadInt32(&rf.curStatus) == Leader
	//log.Printf(log.DTimer, "S%d\tTerm:%v\tCurState:%v", rf.me, rf.currentTerm, rf.curStatus)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int   //候选人的最后日志条目的索引值
	LastLogTerm  int32 //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32        //领导者的任期
	LeaderId     int32        //领导者ID
	PrevLogIndex int          //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int32        //紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntries //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int32        //领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int32 // 当前任期,对于领导者而言 它会更新自己的任期
	Success bool  //结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) waitElectionResult() {
	voteGranted := 1
	for rf.killed() == false {
		select {
		case <-rf.leaveCurElectionChan:
			{
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tCurState:%v\tleaveCurElectionChan", rf.me, rf.currentTerm, rf.curStatus)
				return
			}
		case term := <-rf.voteGrantedChan:
			{
				if term == atomic.LoadInt32(&rf.currentTerm) {
					voteGranted++
					if 2*voteGranted > len(rf.peers) {
						rf.electionDoneChan <- struct{}{}
						return
					}
				} else {
					return
				}
			}
		}
	}
}

func (rf *Raft) election() {
	//* 自增当前的任期号（currentTerm）
	//* 给自己投票
	//* 发送请求投票的 RPC 给其他所有服务器

	atomic.AddInt32(&rf.currentTerm, 1)
	atomic.StoreInt32(&rf.votedFor, rf.me)
	log.Printf(log.DVote, "S%d start election, Term %d", rf.me, rf.currentTerm)
	// sendRequestVote
	args := RequestVoteArgs{
		atomic.LoadInt32(&rf.currentTerm),
		rf.me,
		len(rf.log) - LogIndexOffset,
		rf.log[len(rf.log)-LogIndexOffset].LogTerm,
	}
	for i := 0; i < len(rf.peers); i++ {
		if int32(i) == rf.me {
			continue
		}
		// sendRequestVote
		go func(peerId int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peerId, args, &reply) != false {
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\tRequestVoteReply:%v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
				if reply.Term > atomic.LoadInt32(&rf.currentTerm) {
					log.Printf(log.DTerm, "S%d update Term from %d to S%d", rf.me, reply.Term, rf.currentTerm)
					atomic.StoreInt32(&rf.currentTerm, reply.Term)
					atomic.StoreInt32(&rf.votedFor, VotedForNone)
					rf.replyNewerTermChan <- struct{}{}
					//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treplyNewerTermChan", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus])
					return
				}
				if reply.VoteGranted {
					//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treply.VoteGranted", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus])
					log.Printf(log.DVote, "S%d granted vote from S%d", rf.me, peerId)
					rf.voteGrantedChan <- reply.Term
				}
			}
		}(i, &args)
	}
	// wait for voteGrantedChan or leaveCurElectionChan
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 1.reply false if Term < currentTerm
	log.Printf(log.DTrace, "S%d receive args %v", rf.me, *args)
	//log.Printf(log.DLog2, "S%d receive entries from S%d:%v log:%v", rf.me, args.LeaderId, args.Entries, rf.log)
	//rf.mutexAppendEntries.Lock()
	//defer rf.mutexAppendEntries.Unlock()
	currentTerm := atomic.LoadInt32(&rf.currentTerm)
	commitIndex := atomic.LoadInt32(&rf.commitIndex)
	if currentTerm < args.Term {
		currentTerm = args.Term
		atomic.StoreInt32(&rf.currentTerm, args.Term)
		atomic.StoreInt32(&rf.votedFor, VotedForNone)
		rf.requestNewerTermChan <- struct{}{}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\trequestNewerTermChan", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus])
	}

	if args.Term < currentTerm {
		*reply = AppendEntriesReply{currentTerm, false}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveAppendEntries:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
		log.Printf(log.DTrace, "S%d send AppendEntries reply %v", rf.me, *reply)
		return
	}
	// 2.if log doesn't contain an entry at PrevLogIndex whose Term matches prevLogterm    indexStart = 1 logIndex1 len(log) =2
	if int(args.PrevLogIndex) > len(rf.log)-LogIndexOffset {
		*reply = AppendEntriesReply{currentTerm, false}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveAppendEntries:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
		log.Printf(log.DTrace, "S%d send AppendEntries reply %v", rf.me, *reply)
		return
	}

	//idx logindex
	// 1    1
	// 2    2

	if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		//return // false
		*reply = AppendEntriesReply{currentTerm, false}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveAppendEntries:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
		log.Printf(log.DTrace, "S%d send AppendEntries reply %v", rf.me, *reply)
		return
	}

	// 3. if an exist entry conflicts with a new one ,delete the existing entry and all that follow it
	// 4.append any new Entries not already in the log
	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+LogIndexOffset], args.Entries...)
		log.Printf(log.DLog, "S%d AppendLog from S%d:%v", rf.me, args.LeaderId, rf.log)
	}

	// 5. if LeaderCommit > CommitIndex
	if commitIndex < args.LeaderCommit {
		if int(args.LeaderCommit) < len(rf.log)-LogIndexOffset {
			atomic.StoreInt32(&rf.commitIndex, args.LeaderCommit)
		} else {
			atomic.StoreInt32(&rf.commitIndex, int32(len(rf.log)-LogIndexOffset))
		}
	}
	*reply = AppendEntriesReply{currentTerm, true}
	log.Printf(log.DTrace, "S%d send AppendEntries reply %v", rf.me, *reply)
	rf.curLeaderAppendChan <- struct{}{}
	//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveAppendEntries:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
	return
}

func (rf *Raft) Applied() {
	if atomic.LoadInt32(&rf.commitIndex) > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- struct {
			CommandValid bool
			Command      interface{}
			CommandIndex int
		}{CommandValid: true, Command: rf.log[rf.lastApplied].Cmd, CommandIndex: int(rf.lastApplied)}
		log.Printf(log.DWarn, "S%d applied index %d", rf.me, rf.lastApplied)
	}
}

func (rf *Raft) MainLoop() {
	//log.Printf(log.DTimer, "S%d\tTerm:%v\tMainLoop Start", rf.me, atomic.LoadInt32(&rf.currentTerm))
	log.Printf(log.DTimer, "S%d MainLoop Start", rf.me)
	for rf.killed() == false {
		log.Printf(log.DTerm, "S%d Term is %d,status is %v", rf.me, atomic.LoadInt32(&rf.currentTerm), StatusTypeName[rf.curStatus])
		//log.Printf(log.DLog2,"S%d rand time %v",rf.me,rand.Int63n(ElectionTimeoutSectionStart))
		go rf.Applied()
		switch atomic.LoadInt32(&rf.curStatus) {
		case Follower:
			{
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v", rf.me, atomic.LoadInt32(&rf.currentTerm), )
				select {
				//接收当前领导人的心跳\附加日志 任期需大于等于当前任期
				case <-rf.replyNewerTermChan:
					{
						//rf.curStatus = Follower
					}
				case <-rf.requestNewerTermChan:
					{
						//rf.curStatus = Follower
					}
				case <-rf.curLeaderAppendChan:
					{
						// status = Follower
						// 收到有效领导人的LogEntries 刷新election timeout
						// do handleLogEntries
					}
				case <-rf.hasVotedChan:
					{
						// status = Follower
						// 投过票 刷新election timeout
						// be follower
					}
				// Candidate status
				case <-time.After((time.Duration(rand.Int63n(ElectionTimeoutSectionStart)) + ElectionTimeoutSectionDuration) * time.Millisecond):
					{
						log.Printf(log.DWarn, "S%d election timeout", rf.me)
						//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\tElectionTimeout", rf.me, atomic.LoadInt32(&rf.currentTerm), StatusTypeName[Candidate])
						atomic.StoreInt32(&rf.curStatus, Candidate)
						rf.leaveFollowerChan <- struct{}{}
					}
				}
			}
		case Candidate:
			{
				//log.Printf(log.DTerm, "S%d Term is %d,", rf.me, atomic.LoadInt32(&rf.currentTerm))
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v", rf.me, atomic.LoadInt32(&rf.currentTerm), StatusTypeName[Candidate])
				// Election timeout
				// becomes a candidate and starts a new election Term
				go rf.election()
				go rf.waitElectionResult()
				// 在转变成候选人后就立即开始选举过程
				//* 重置选举超时计时器
				select {
				// 选举完成 成为Leader
				case <-rf.electionDoneChan:
					{
						atomic.StoreInt32(&rf.curStatus, Leader)
						// 发送空的附加日志 RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以阻止跟随者超时
						//初始化所有的 nextIndex 值为自己的最后一条日志的 indexStart 加 1
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
						}
						log.Printf(log.DVote, "S%d Request vote finished and become leader", rf.me)
						rf.sendHeartBeats()
						go rf.waitAppendResult()
					}
				// 转为Follower
				case <-rf.replyNewerTermChan:
					{
						atomic.StoreInt32(&rf.curStatus, Follower)
						//log.Println("Candidate rf.curStatus = Follower")
						rf.leaveCurElectionChan <- struct{}{}
					}

				case <-rf.requestNewerTermChan:
					{
						atomic.StoreInt32(&rf.curStatus, Follower)
						//log.Println("Candidate rf.curStatus = Follower")
						rf.leaveCurElectionChan <- struct{}{}
					}

				// 选举超时
				case <-time.After((time.Duration(rand.Int63n(ElectionTimeoutSectionStart)) + ElectionTimeoutSectionDuration) * time.Millisecond):
					{
						// status = Candidate
						rf.leaveCurElectionChan <- struct{}{}
						log.Printf(log.DWarn, "S%d election timeout", rf.me)
					}
				}
			}

		case Leader:
			{
				//log.Printf(log.DTerm, "S%d Term is %d,", rf.me, atomic.LoadInt32(&rf.currentTerm))
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v", rf.me, atomic.LoadInt32(&rf.currentTerm), StatusTypeName[Leader])
				select {
				case <-time.After(100 * time.Millisecond):
					{
						log.Printf(log.DInfo, "S%d send HeartBeats", rf.me)
						//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\tsendHeartBeats", rf.me, atomic.LoadInt32(&rf.currentTerm), StatusTypeName[Leader])
						rf.sendHeartBeats()
					}

				case <-rf.requestNewerTermChan:
					{
						atomic.StoreInt32(&rf.curStatus, Follower)
						rf.leaveLeaderChan <- struct{}{}
						log.Printf(log.DInfo, "S%d Leave Leader", rf.me)
					}
				case <-rf.replyNewerTermChan:
					{
						atomic.StoreInt32(&rf.curStatus, Follower)
						rf.leaveLeaderChan <- struct{}{}
						log.Printf(log.DInfo, "S%d Leave Leader", rf.me)
					}
				}

			}
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果`Term < currentTerm`返回 false
	log.Printf(log.DVote, "S%d receive RequestVoteArgs %+v", rf.me, *args)
	votedFor := atomic.LoadInt32(&rf.votedFor)
	//curStatus := atomic.LoadInt32(&rf.curStatus)
	currentTerm := atomic.LoadInt32(&rf.currentTerm)

	if currentTerm > args.Term {
		*reply = RequestVoteReply{
			currentTerm, false,
		}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveRequestVote:%+v\t%+v", rf.me, currentTerm, StatusTypeName[curStatus], args, reply)
		return
	}

	if currentTerm < args.Term {
		currentTerm = args.Term
		votedFor = VotedForNone
		atomic.StoreInt32(&rf.currentTerm, currentTerm)
		atomic.StoreInt32(&rf.votedFor, VotedForNone)
		rf.requestNewerTermChan <- struct{}{}
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\trequestNewerTermChan", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus])
	}

	// 如果 votedFor 为空或者为 CandidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	if (votedFor == VotedForNone || votedFor == args.CandidateId) && int32(args.LastLogIndex) >= atomic.LoadInt32(&rf.commitIndex) {
		atomic.StoreInt32(&rf.votedFor, args.CandidateId)
		rf.hasVotedChan <- struct{}{}
		*reply = RequestVoteReply{
			currentTerm, true,
		}
		log.Printf(log.DVote, "S%d vote S%d", rf.me, args.CandidateId)
		//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveRequestVote:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
		return
	}
	*reply = RequestVoteReply{
		currentTerm, false,
	}
	//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treceiveRequestVote:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the indexStart of the target server in rf.peers[].
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
// the first return value is the indexStart that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) waitAppendResult() {
	countAppend := make(map[int]int)
	for rf.killed() == false {
		select {
		case <-rf.leaveLeaderChan:
			return
			
		case reply := <-rf.appendSucceedChan:
			{
				log.Printf(log.DCommit, "S%d %+v", rf.me, reply)
				rf.nextIndex[reply.peerId] = reply.nextIndex
				rf.matchIndex[reply.peerId] = reply.nextIndex - 1
				for i := reply.indexStart; i < reply.nextIndex; i++ {
					_, ok := countAppend[i]
					if ok {
						countAppend[i]++
					} else {
						countAppend[i] = 2
					}
					if 2*countAppend[i] > len(rf.peers) {
						//fmt.Println(len(rf.log),reply)
						if int32(reply.nextIndex-1) > atomic.LoadInt32(&rf.commitIndex) {
							//atomic
							log.Printf(log.DCommit, "S%d CommitIndex Update from %d to %d", rf.me, atomic.LoadInt32(&rf.commitIndex), reply.nextIndex-1)
							atomic.StoreInt32(&rf.commitIndex, int32(reply.nextIndex-1))
							//rf.applyCh <- struct {
							//	CommandValid bool
							//	Command      interface{}
							//	CommandIndex int
							//}{CommandValid: true, Command: rf.log[i].Cmd, CommandIndex: i}
							//log.Printf(log.DTrace, "S%d applyCh %d", rf.me, i)
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) sendHeartBeats() {

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == int32(i) {
			continue
		}
		var args AppendEntriesArgs
		if len(rf.log)-LogIndexOffset >= rf.nextIndex[i] {
			args = AppendEntriesArgs{atomic.LoadInt32(&rf.currentTerm),
				rf.me,
				rf.nextIndex[i] - LogIndexOffset,
				rf.log[rf.nextIndex[i]-LogIndexOffset].LogTerm,
				rf.log[rf.nextIndex[i]:],
				atomic.LoadInt32(&rf.commitIndex),
			}
		} else {
			args = AppendEntriesArgs{atomic.LoadInt32(&rf.currentTerm),
				rf.me,
				rf.nextIndex[i] - LogIndexOffset,
				rf.log[rf.nextIndex[i]-LogIndexOffset].LogTerm,
				[]LogEntries{},
				atomic.LoadInt32(&rf.commitIndex),
			}
		}

		log.Printf(log.DLeader, "S%d send AppendEntries to S%d:%v", rf.me, i, args)
		go func(peerId int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(peerId, args, &reply) != false {
				log.Printf(log.DLeader, "S%d get AppendEntriesReply from S%d:%v", rf.me, peerId, reply)
				//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\tAppendEntriesReply:%+v\t%+v", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus], args, reply)
				if reply.Term > atomic.LoadInt32(&rf.currentTerm) {
					log.Printf(log.DTerm, "S%d update Term from %d to %d", rf.me, reply.Term, rf.currentTerm)
					atomic.StoreInt32(&rf.currentTerm, reply.Term)
					atomic.StoreInt32(&rf.votedFor, VotedForNone)
					rf.replyNewerTermChan <- struct{}{}
					log.Printf(log.DTerm, "S%d rf.replyNewerTermChan <- struct{}{} curstatue=%v", rf.me, rf.curStatus)
					//log.Printf(log.DTimer, "S%d\tTerm:%v\tStatus:%v\treplyNewerTermChan", rf.me, rf.currentTerm, StatusTypeName[rf.curStatus])
					return
				}
				if reply.Success {
					//log.Printf(log.DVote, "S%d success append indexStart %d to S%d", rf.me, indexStart,peerId)
					//fmt.Println(len(rf.peers),peerId, len(rf.nextIndex))
					if len(args.Entries) != 0 {
						rf.appendSucceedChan <- struct {
							peerId     int
							indexStart int
							nextIndex  int
						}{peerId: peerId, indexStart: rf.nextIndex[peerId], nextIndex: len(rf.log)} //temp
					}

				} else {
					//fmt.Println("rf.nextIndex[peerId]--",rf.nextIndex[peerId])
					rf.nextIndex[peerId]--
				}
			}
		}(i, &args)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	isLeader := atomic.LoadInt32(&rf.curStatus) == Leader
	if isLeader == false {
		return 0, 0, isLeader
	}
	// Your code here (2B).
	index := len(rf.log)
	//fmt.Println("indexStart = ",indexStart)
	term := int(atomic.LoadInt32(&rf.currentTerm))

	//append log for self
	rf.log = append(rf.log[:len(rf.log)], struct {
		LogTerm int32
		Cmd     interface{}
	}{LogTerm: int32(term), Cmd: command})

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
	rf := &Raft{
		peers:                peers,
		persister:            persister,
		me:                   int32(me),
		curStatus:            Follower,
		votedFor:             VotedForNone,
		log:                  make([]LogEntries, 0),
		commitIndex:          0,
		hasVotedChan:         make(chan struct{}, 1),
		electionDoneChan:     make(chan struct{}, 1),
		leaveCurElectionChan: make(chan struct{}, 1),
		leaveFollowerChan:    make(chan struct{}, 1),
		voteGrantedChan:      make(chan int32, len(peers)),
		requestNewerTermChan: make(chan struct{}, len(peers)),
		replyNewerTermChan:   make(chan struct{}, len(peers)),
		curLeaderAppendChan:  make(chan struct{}, 1),
		leaveLeaderChan:      make(chan struct{}, 1),
		appendSucceedChan:    make(chan PeerIndex, len(peers)),
		applyCh:              applyCh,
		lastApplied:          0,
		nextIndex:            make([]int, len(peers)),
		matchIndex:           make([]int, len(peers)),
	}
	rf.log = append(rf.log, struct {
		LogTerm int32
		Cmd     interface{}
	}{LogTerm: 0, Cmd: 0})
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	//log.Println("rf.me:", me)

	go rf.MainLoop()
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
