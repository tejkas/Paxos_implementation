package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Operation         string
	Client_request_id int64
	Key               string
	Value             string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	m_kvmap            map[string]string
	m_log              []Op
	seq_id             int
	client_request_map map[int64]bool
}

func (kv *KVPaxos) pause(seq_id int) Op {

	to := 10 * time.Millisecond
	for {
		status, op_details := kv.px.Status(seq_id)
		if status == paxos.Decided {

			var m_details Op
			m_details = op_details.(Op)

			return m_details
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) updateLog(addOp *Op) {

	// if addOp.Operation == GET {
	// 	//kv.m_log = append(kv.m_log, *addOp)
	// 	proceed = true
	// }

	if addOp.Operation == PUT {
		//kv.m_log = append(kv.m_log, *addOp)
		kv.m_kvmap[addOp.Key] = addOp.Value
	}

	if addOp.Operation == APPEND {

		//kv.m_log = append(kv.m_log, *addOp)

		if val, ok := kv.m_kvmap[addOp.Key]; ok {

			kv.m_kvmap[addOp.Key] = val + addOp.Value

		} else {

			kv.m_kvmap[addOp.Key] = addOp.Value
		}

	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()

	if _, ok := kv.client_request_map[args.Client_request_id]; ok {

		reply.Err = OK
		reply.Value = kv.m_kvmap[args.Key] //should work?
		kv.mu.Unlock()
		return nil
	}

	//fmt.Println("ENTERED GET HANDLER")
	currOp := new(Op)
	currOp.Operation = GET
	currOp.Client_request_id = args.Client_request_id
	currOp.Key = args.Key
	currOp.Value = ""
	//Make sure that this hasn't already occured, and in doing so keep the log updated
	var addOp Op
	for {
		fate, op_details := kv.px.Status(kv.seq_id)

		// var addOp Op
		if fate == paxos.Decided {

			addOp = op_details.(Op)

		} else {

			kv.px.Start(kv.seq_id, *currOp)
			addOp = kv.pause(kv.seq_id)
		}

		kv.updateLog(&addOp)

		kv.px.Done(kv.seq_id)
		kv.client_request_map[addOp.Client_request_id] = true
		kv.seq_id = kv.seq_id + 1

		if addOp.Client_request_id == currOp.Client_request_id {
			break
		}

	}

	//assuming key value storeage is good now
	if val, ok := kv.m_kvmap[currOp.Key]; ok {

		reply.Value = val
		reply.Err = OK
	} else {

		reply.Err = ErrNoKey
		reply.Value = ""
	}

	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	kv.mu.Lock()
	//Check duplicates

	//fmt.Println("ENTERED PUT APPEND HANDLER")

	if _, ok := kv.client_request_map[args.Client_request_id]; ok {

		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}

	currOp := new(Op)
	currOp.Operation = args.Op
	currOp.Client_request_id = args.Client_request_id
	currOp.Key = args.Key
	currOp.Value = args.Value
	//fmt.Println(currOp.Client_request_id)

	var addOp Op

	//Make sure that this hasn't already occured, and in doing so keep the log updated
	//fmt.Println("ENTERING LOOP")
	for {
		fate, op_details := kv.px.Status(kv.seq_id)

		//addOp := new(Op)
		// var addOp Op
		if fate == paxos.Decided {

			addOp = op_details.(Op)

		} else {

			//fmt.Println("STARTING EXECUTION")
			kv.px.Start(kv.seq_id, *currOp)
			addOp = kv.pause(kv.seq_id) //, addOp)
			//fmt.Println("GOT THE DECIDED OP")
			//fmt.Println(addOp.Client_request_id)
		}

		kv.updateLog(&addOp)
		kv.px.Done(kv.seq_id)
		kv.client_request_map[addOp.Client_request_id] = true
		kv.seq_id = kv.seq_id + 1

		if addOp.Client_request_id == currOp.Client_request_id {
			break
		}

	}

	reply.Err = OK
	kv.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.m_kvmap = make(map[string]string)
	kv.client_request_map = make(map[int64]bool)
	kv.m_log = [] Op{}
	kv.seq_id = 1

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
