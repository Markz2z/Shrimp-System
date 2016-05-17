package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type OpType int

const (
	OpGet        = 0
	OpAppend = 1
	OpPut         = 2
)

type Op struct {
	Type    OpType
	Key      string
	Value   string
	Client  int64
	Id         int64
}

type OpReply struct {
	WrongLeader bool
	Err            Err
	Value       string
}