package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.

	instances    map[int]*Instance
	majority     int
	AcceptedSeqs []int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func (px *Paxos) PrepareID() string {

	// px.mu.Lock()	could lead to deadlock :(

	basis := time.Date(2019, time.May, 1, 0, 0, 0, 0, time.UTC)
	elasped := time.Now().Sub(basis).Nanoseconds()
	id := strconv.FormatInt(elasped, 10) + "." + strconv.Itoa(px.me)

	// px.mu.Unlock()
	return id
}

func (px *Paxos) PreparePhase(args *PrepareArgs, reply *PrepareReply) error {

	px.mu.Lock()

	if val, ok := px.instances[args.SeqNum]; ok {

		if args.ProposeNum <= val.propose {
			reply.Err = Reject
			px.mu.Unlock()
			return nil
		} else {
			reply.Err = OK
			px.instances[args.SeqNum].propose = args.ProposeNum

			// reply.ProposeNum = val.propose_num
			reply.AcceptNum = val.accept
			// reply.SeqNum = args.SeqNum // Do we need SeqNum in reply?
			reply.Value = val.value
		}
	} else {
		reply.Err = OK
		i := Instance{accept: "", propose: "", value: nil, status: Pending}
		px.instances[args.SeqNum] = &i
		px.instances[args.SeqNum].propose = args.ProposeNum

		// reply.ProposeNum = ""
		reply.AcceptNum = ""
		// reply.SeqNum = args.SeqNum
		reply.Value = nil
	}

	px.mu.Unlock()
	return nil
}

func (px *Paxos) RunPrepare(seq int, v interface{}, packet *Packet) bool {

	pid := px.PrepareID()
	voteCount := 0
	currentPid := ""
	currentVal := v

	parg := new(PrepareArgs)
	parg.SeqNum = seq
	parg.ProposeNum = pid

	for _, val := range px.peers {

		preply := new(PrepareReply)
		preply.AcceptNum = ""
		preply.Value = nil
		preply.Err = Reject
		// preply.ProposeNum = pid
		// preply.SeqNum = seq

		call(val, "Paxos.PreparePhase", &parg, &preply)

		if preply.Err == OK {
			voteCount = voteCount + 1

			if currentPid < preply.AcceptNum {
				currentPid = preply.AcceptNum
				currentVal = preply.Value
			}
		}

	}

	packet.ProposeNum = pid
	packet.Value = currentVal

	if voteCount >= px.majority {
		return true
	} else {
		return false
	}

}

func (px *Paxos) AcceptPhase(args *AcceptArgs, reply *AcceptReply) error {

	px.mu.Lock()

	if val, ok := px.instances[args.SeqNum]; ok {

		if args.ProposeNum < val.propose {
			reply.Err = Reject
			px.mu.Unlock()
			return nil
		} else {
			reply.Err = OK
			px.instances[args.SeqNum].accept = args.ProposeNum
			px.instances[args.SeqNum].propose = args.ProposeNum
			px.instances[args.SeqNum].value = args.Value
		}
	} else {
		i := Instance{accept: args.ProposeNum, propose: args.ProposeNum, value: args.Value, status: Pending}
		px.instances[args.SeqNum] = &i
	}

	px.mu.Unlock()
	return nil
}

func (px *Paxos) RunAccept(seq int, propose string, v interface{}) bool {

	voteCount := 0
	aarg := new(AcceptArgs)
	// aarg.AcceptNum =
	aarg.ProposeNum = propose
	aarg.SeqNum = seq
	aarg.Value = v

	for i, val := range px.peers {

		areply := new(AcceptReply)

		// fmt.Println("About to call...")
		// call(val, "Paxos, AcceptPhase", &aarg, &areply)
		// fmt.Println("Finished Call...")

		if i != px.me {
			call(val, "Paxos.AcceptPhase", &aarg, &areply)
		} else {
			px.AcceptPhase(aarg, areply)
		}

		if areply.Err == OK {
			voteCount = voteCount + 1
		}
	}

	if voteCount >= px.majority {
		return true
	} else {
		return false
	}
}

// Breakpoint

func (px *Paxos) LearnPhase(args *LearnArgs, reply *LearnReply) error {

	px.mu.Lock()

	if _, ok := px.instances[args.SeqNum]; ok {

		px.instances[args.SeqNum].accept = args.ProposeNum
		px.instances[args.SeqNum].propose = args.ProposeNum
		px.instances[args.SeqNum].value = args.Value
		px.instances[args.SeqNum].status = Decided

		px.AcceptedSeqs[args.Me] = args.MaxFinishedSeq

	} else {
		i := Instance{accept: args.ProposeNum, propose: args.ProposeNum, value: args.Value, status: Decided}
		px.instances[args.SeqNum] = &i

		px.AcceptedSeqs[args.Me] = args.MaxFinishedSeq
	}

	px.mu.Unlock()
	return nil
}

func (px *Paxos) RunLearn(seq int, propose string, v interface{}) {

	px.mu.Lock()

	// if _, ok := px.instances[seq]; !ok {
	// 	fmt.Println("Not ok!")
	// 	fmt.Println(seq)
	// }

	if _, ok := px.instances[seq]; !ok {
		i := new(Instance)
		px.instances[seq] = i
	}

	px.instances[seq].accept = propose
	px.instances[seq].propose = propose
	px.instances[seq].value = v
	px.instances[seq].status = Decided

	// Above is all the critical region. Had to unlock here
	// to avoid deadlock when calling itself to learn...
	px.mu.Unlock()

	larg := new(LearnArgs)
	larg.MaxFinishedSeq = px.AcceptedSeqs[px.me]
	larg.Me = px.me
	larg.ProposeNum = propose
	larg.SeqNum = seq
	larg.Value = v

	for i, peer := range px.peers {

		if i != px.me {

			lreply := new(LearnReply)
			call(peer, "Paxos.LearnPhase", &larg, &lreply)
		}
	}
}

func (px *Paxos) RunPaxos(seq int, v interface{}) {

	if px.Min() > seq {
		return
	} else {
		for {
			// ret := false
			packet := new(Packet)

			if px.RunPrepare(seq, v, packet) {

				if px.RunAccept(seq, packet.ProposeNum, packet.Value) {

					px.RunLearn(seq, packet.ProposeNum, packet.Value)
					break

				}
			}

			if status, _ := px.Status(seq); status == Decided {
				break
			}
		}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go px.RunPaxos(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.

	px.mu.Lock()

	if px.AcceptedSeqs[px.me] < seq {
		px.AcceptedSeqs[px.me] = seq
	}

	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.

	px.mu.Lock()

	maxSeq := -1

	for key, _ := range px.instances {
		if maxSeq < key {
			maxSeq = key
		}
	}

	px.mu.Unlock()
	return maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.

	// fmt.Println("Got Here 4")

	px.mu.Lock()

	// fmt.Println("Got Here 5")

	minSeq := px.AcceptedSeqs[px.me]

	for i := range px.AcceptedSeqs {

		if minSeq > px.AcceptedSeqs[i] {
			minSeq = px.AcceptedSeqs[i]
		}

	}

	for key, val := range px.instances {
		if key <= minSeq && val.status == Decided {
			delete(px.instances, key)
		}
	}

	minSeq = minSeq + 1

	px.mu.Unlock()
	return minSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	// fmt.Println("Got Here 3")

	// px.mu.Lock()	// Causing deadlock

	if px.Min() > seq {
		// fmt.Println("Got Here 4")
		// px.mu.Unlock()
		return Forgotten, nil
	}

	// fmt.Println("Got Here 4")

	px.mu.Lock()

	if val, ok := px.instances[seq]; ok {
		px.mu.Unlock()
		return val.status, val.value
	} else {
		px.mu.Unlock()
		return Pending, nil
	}

	px.mu.Unlock()
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.

	px.majority = (len(px.peers) / 2) + 1
	px.instances = make(map[int]*Instance)
	px.AcceptedSeqs = make([]int, len(px.peers))
	for i := range px.peers {
		px.AcceptedSeqs[i] = -1
	}

	// End of initialization

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					//fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
