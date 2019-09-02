package paxos

const (
	OK     = "OK"
	Reject = "Reject"
)

type Instance struct {
	propose string //highest prepare number proposed
	accept  string //currently accepted num
	value   interface{}
	status  Fate
}

type Packet struct {
	ProposeNum string
	Value      interface{}
}

type PrepareArgs struct {
	SeqNum     int //which instance is it for
	ProposeNum string
}

type PrepareReply struct {
	// SeqNum int
	// ProposeNum string
	AcceptNum string
	Value     interface{}
	Err       string
}

type AcceptArgs struct {
	SeqNum     int
	ProposeNum string
	// AcceptNum  string
	Value interface{}
}

type AcceptReply struct {
	// SeqNum    int
	// AcceptNum string
	// Value     interface{}
	Err string
}

type LearnArgs struct {
	SeqNum     int
	ProposeNum string
	// AcceptNum  string
	Value          interface{}
	Me             int
	MaxFinishedSeq int
}

// Learn does not need reply. But call()
// needs it...
type LearnReply struct {
	Err string
}
