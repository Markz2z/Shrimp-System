package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

const (
	OpGet        = 0
	OpAppend = 1
	OpPut         = 2
)

type Op struct {
	Type    int
	Key      string
	Value   string
	Client  int64
	Id         int64
}

type OpReply struct {
	IsLeader bool
	Err            Err
	Value       string
}