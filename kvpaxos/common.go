package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_request_id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_request_id int64
}

type GetReply struct {
	Err   Err
	Value string
}
