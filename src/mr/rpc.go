package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type StatusType int8
type TaskType   int8

const (
	INIT         StatusType = 0
	MAPPING      StatusType = 1
	MAPOK        StatusType = 2
	MAPFAIL      StatusType = 3
	REDUCING     StatusType = 4
	REDUCEOK     StatusType = 5
	REDUCEFAIL   StatusType = 6
	COMPLETED    StatusType = 7
	ERROR        StatusType = 8

	MAP_TYPE     int8 = 0
	REDUCE_TYPE  int8 = 1
)

type Task struct {
	TaskID string
	FileName string
	Status StatusType
	CreateTime time.Time
	ModifyTime time.Time
	TaskType int8

	NReduce int
	RetryTimes int
}

type TaskArgs struct {
	MasterID string
	WorkerID string

	Task Task
}

type TaskReply TaskArgs

type RegisterWorker struct {
	Worker Worker
	Status bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
