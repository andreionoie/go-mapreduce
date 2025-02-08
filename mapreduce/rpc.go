package mapreduce

import (
	"log"
	"net/rpc"
)

type TaskType int

const (
	Map TaskType = 1 << iota
	Reduce
)

func (t TaskType) String() string {
	switch t {
	case Map:
		return "MapTask"
	case Reduce:
		return "ReduceTask"
	default:
		return "UnknownTask"
	}
}

type TaskResponseType int

const (
	AssignedTask TaskResponseType = 1 << iota
	WaitingForPendingMaps
	WaitingForPendingReduces
	AllTasksFinished
	ErrorFindingTask
)

func (t TaskResponseType) String() string {
	switch t {
	case AssignedTask:
		return "AssignedTask"
	case WaitingForPendingMaps:
		return "WaitingForPendingMapperTasks"
	case WaitingForPendingReduces:
		return "WaitingForPendingReduceTasks"
	case AllTasksFinished:
		return "AllTasksFinished"
	default:
		return "UnknownTaskResponse"
	}
}

type TaskState int

const (
	Idle TaskState = 1 << iota
	InProgress
	Completed
	Error
)

func (s TaskState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	case Error:
		return "Error"
	default:
		return "UnknownState"
	}
}

type WorkerTaskUpdateArgs struct {
	Type       TaskType
	TaskNumber int
	WorkerName string

	NewState        TaskState
	OutputFilePaths []string
}

type WorkerTaskStatusReply struct {
	Response TaskResponseType

	Type       TaskType
	TaskNumber int
	TaskState  TaskState
	WorkerName string

	InputFilePaths  []string
	OutputFilePaths []string
}

type WorkerTaskRequestArgs struct {
	WorkerName string
}

type WorkerAssignedTaskReply struct {
	Response TaskResponseType

	Type           TaskType
	TaskNumber     int
	NReduce        int
	InputFilePaths []string
}

func CallMaster(rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", masterSocket())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		log.Print(err)
	}

	return err == nil
}

func masterSocket() string {
	return "/var/tmp/mapreduce-master.sock"
}
