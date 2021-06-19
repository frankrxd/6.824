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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
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
type IndexType int
type TermType int
type ElectionStatusType int

const (
	Follower = 0
	Candidate = 1
	Leader = 2
	ElectionTimeoutSectionStart int64 = 150
	ElectionTimeoutSectionDuration time.Duration = 150
	VotedForNone = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persister statue
	currentTerm TermType // 服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor int //	当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	log []LogEntries //日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）

	// volatility statue
	commitIndex IndexType
	lastApplied IndexType
	// leader volatility statue
	nextIndex []IndexType //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []IndexType //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	electionStatus ElectionStatusType

	electionChan chan struct{}
}

type LogEntries struct {
	LogTerm TermType
}

type LogStatus struct {
	LogIndex IndexType
	LogTerm	TermType
}


type AppendEntriesArgs struct {
	term TermType
	leaderId int
	prevLogIndex IndexType
	prevLogTerm TermType
	entries []LogEntries
	leaderCommit IndexType
}

type AppendEntriesReply struct {
	term TermType
	success bool
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
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
	term TermType
	candidateId int
	lastLogIndex IndexType
	lastLogTerm TermType
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term TermType
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) ElectionTimeoutLoop() {
	status := Follower
	for {
		switch status {
		case Follower: {
			select {
			//接收心跳包
			case <-rf.electionChan: {

			}

			// Candidate status
			case <-time.After(( time.Duration(rand.Int63n(ElectionTimeoutSectionStart))  + ElectionTimeoutSectionDuration) * time.Millisecond): {
				status = Candidate
			}

			}
		}
		case Candidate: {
			// Election timeout
			// becomes a candidate and starts a new election term
			select {
			// 选举完成
			case <- CandidateDone {
				status = Leader
			}
			// 转为Follower
			case <- SwitchFollower {
				status = Follower
			}

			// 选举超时
			case <-time.After(( time.Duration(rand.Int63n(ElectionTimeoutSectionStart))  + ElectionTimeoutSectionDuration) * time.Millisecond): {
				 // status = Candidate
				选举超时 <- struct{}
			}
			}

			// 选举过程
			go func() {
				go func() {
					for {
						voteGranted := 1
						select {
						case <- 选举超时 { return }
						case <- voteGrantedChan {
							voteGranted ++
							if 2*voteGranted > len(rf.peers) {	CandidateDone<-struct{}	}
						}
						//接收心跳包
						case <-rf.electionChan: {
							//如果这个领导人的任期号（包含在此次的 RPC中）不小于候选人当前的任期号，那么候选人会承认领导人合法并回到跟随者状态。
							SwitchFollower <- struct{}
							//如果此次 RPC 中的任期号比自己小，那么候选人就会拒绝这次的 RPC 并且继续保持候选人状态。
						}
						}
					}
				}()
				rf.currentTerm ++
				rf.votedFor = rf.me
				voteGranted := 1
				// sendRequestVote
				for i:=0;i<len(rf.peers);i++ {
					if i == rf.me {
						continue
					}
					args := RequestVoteArgs{
						rf.currentTerm,
						rf.me,
						IndexType(len(rf.log)),
						rf.log[len(rf.log)-1].LogTerm,
					}
					reply := RequestVoteReply{}
					if rf.sendRequestVote(i,&args,&reply) != false {
						if reply.voteGranted {
							voteGranted ++
						}
					}
				}
			}()


		}

		case Leader: {

		}

		}

	}
}


func isLogNewer(log1 LogStatus,log2 LogStatus) bool{
	if log1.LogTerm > log2.LogTerm {
		return true
	} else if log1.LogTerm == log2.LogTerm && log1.LogIndex >= log2.LogIndex{
		return true
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果`term < currentTerm`返回 false
	if rf.currentTerm > args.term {
		*reply = RequestVoteReply{
			rf.currentTerm,false,
		}
		return
	}
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	if (rf.votedFor == VotedForNone || rf.votedFor == args.candidateId) &&
		isLogNewer(LogStatus{args.lastLogIndex,args.lastLogTerm},LogStatus{IndexType(len(rf.log)),rf.log[len(rf.log)-1].LogTerm})  {
		rf.votedFor = args.candidateId
		if rf.currentTerm != args.term {
			rf.currentTerm = args.term
			rf.votedFor = VotedForNone
		}
		*reply = RequestVoteReply{
			rf.currentTerm,true,
		}
		return
	}
	*reply = RequestVoteReply{
		rf.currentTerm,false,
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	log.Println("rf.peers:",peers)
	log.Println("rf.persister:",persister)
	log.Println("rf.me:",me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
