package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Master struct {
	R           int // the number of partitions / reduce tasks, specified by the user
	WorkerTasks []WorkerTask
	tasksMutex  sync.Mutex
}

type WorkerTask struct {
	Type           TaskType
	State          TaskState
	TaskNumber     int
	InputFilePaths []string

	OutputFilePaths    []string
	AssignedWorkerName string
	AssignedTimestamp  time.Time
}

func (t *WorkerTask) isExpired() bool {
	return t.State == InProgress && len(t.AssignedWorkerName) != 0 && time.Since(t.AssignedTimestamp) > 10*time.Second
}

func (m *Master) addTask(tsk WorkerTask) {
	m.WorkerTasks = append(m.WorkerTasks, tsk)
}

func (m *Master) countTasks(stateMask TaskState, typeMask TaskType) int {
	cnt := 0
	for i := range m.WorkerTasks {
		if m.WorkerTasks[i].Type&typeMask != 0 && m.WorkerTasks[i].State&stateMask != 0 {
			cnt++
		}
	}

	return cnt
}

func (m *Master) getIdleTask() *WorkerTask {
	for i := range m.WorkerTasks {
		if m.WorkerTasks[i].State == Idle {
			return &m.WorkerTasks[i]
		}
	}
	panic("no idle tasks found")
}

func (m *Master) allTasksFinished() bool {
	noTasksPending := m.countTasks(Idle|InProgress, Map|Reduce) == 0
	allReduceTasksFinished := m.countTasks(Completed|Error, Reduce) > 0

	return noTasksPending && allReduceTasksFinished
}

func (m *Master) WorkerTaskUpdate(args *WorkerTaskUpdateArgs, reply *WorkerTaskStatusReply) error {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()

	log.Printf("Searching for task to update: %v", *args)
	var foundTask *WorkerTask
	for i := range m.WorkerTasks {
		task := &m.WorkerTasks[i]
		if task.Type == args.Type && task.TaskNumber == args.TaskNumber {
			foundTask = task
		}
	}
	if foundTask == nil {
		panic("no such task found for update")
	}
	log.Printf("L-> Found task to update: %v", *foundTask)

	reply.Type = foundTask.Type
	reply.TaskNumber = foundTask.TaskNumber
	reply.TaskState = foundTask.State
	reply.WorkerName = foundTask.AssignedWorkerName

	reply.InputFilePaths = foundTask.InputFilePaths
	reply.OutputFilePaths = foundTask.OutputFilePaths

	if foundTask.AssignedWorkerName != args.WorkerName {
		log.Print("L-> worker names do not match for task update, ignoring..")
		return nil
	}

	if foundTask.State != InProgress {
		panic("unexpected state for task update")
	}

	foundTask.State = args.NewState
	foundTask.OutputFilePaths = args.OutputFilePaths

	reply.TaskState = foundTask.State
	reply.OutputFilePaths = foundTask.OutputFilePaths

	// TODO: improve this, should trigger when last Map task is finished
	if foundTask.Type == Map && m.countTasks(Idle|InProgress, Map) == 0 {
		go m.createReduceTasks()
	}
	if foundTask.Type == Reduce && m.countTasks(Idle|InProgress, Reduce) == 0 {
		log.Print("Last reduce task finished, exiting soon...")
		reply.Response = AllTasksFinished
	}

	return nil
}

func (m *Master) WorkerTaskRequest(args *WorkerTaskRequestArgs, reply *WorkerAssignedTaskReply) error {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()

	if m.countTasks(Idle, Map|Reduce) > 0 {
		idleTask := m.getIdleTask()

		// update the task
		idleTask.State = InProgress
		idleTask.AssignedWorkerName = args.WorkerName
		idleTask.AssignedTimestamp = time.Now()
		log.Printf("Assigned task to worker: %v", *idleTask)
		// populate reply
		reply.Type = idleTask.Type
		reply.TaskNumber = idleTask.TaskNumber
		reply.InputFilePaths = idleTask.InputFilePaths
		reply.NReduce = m.R

		reply.Response = AssignedTask
	} else if m.countTasks(InProgress|Completed|Error, Reduce) == 0 {
		reply.Response = WaitingForPendingMaps
	} else if m.countTasks(InProgress, Reduce) > 0 {
		reply.Response = WaitingForPendingReduces
		if m.countTasks(Idle|InProgress|Completed|Error, Map) != m.countTasks(Completed|Error, Map) {
			panic("unexpected map tasks still pending when reduce tasks are also in pending")
		}
	} else if m.allTasksFinished() {
		reply.Response = AllTasksFinished
	} else {
		reply.Response = ErrorFindingTask
		log.Printf(" -------- ERROR SELECTING TASK OUT OF %v", m.WorkerTasks)
	}
	return nil
}

func getReduceTaskNumber(filename string) int {
	var mapTaskNumber, reduceTaskNumber int
	if _, err := fmt.Sscanf(filename, "intermediate-M%d-R%d.json", &mapTaskNumber, &reduceTaskNumber); err != nil {
		panic(fmt.Errorf("failed to parse %q: %w", filename, err))
	}
	return reduceTaskNumber
}

func (m *Master) createReduceTasks() {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()
	// TODO: add dummy task so that Done() still returns false
	reduceTasks := make(map[int]*WorkerTask)

	for i := range m.WorkerTasks {
		mapperTask := &m.WorkerTasks[i]
		if mapperTask.Type != Map {
			panic("expected only map task types at this stage")
		}
		if mapperTask.State != Completed {
			panic("found incomplete mapper task")
		}
		for _, intermediateFilePath := range mapperTask.OutputFilePaths {
			// the Y part of mapreduce-MX-RY.json
			idx := getReduceTaskNumber(filepath.Base(intermediateFilePath))
			if idx < 0 || idx >= m.R {
				panic("unexpected reduce task number extracted from filename")
			}
			// create new task if not already populated
			if _, ok := reduceTasks[idx]; !ok {
				reduceTasks[idx] = &WorkerTask{
					Type:       Reduce,
					State:      Idle,
					TaskNumber: idx,
				}
			}
			// add the intermediate file to the list
			reduceTasks[idx].InputFilePaths = append(reduceTasks[idx].InputFilePaths, intermediateFilePath)
		}
	}

	log.Print("Created following reduce tasks:")
	for _, reduceTask := range reduceTasks {
		m.addTask(*reduceTask)
		log.Printf("L> %v", *reduceTask)
	}
}

func (m *Master) startTasksDaemon() {
	go func() {
		for {

			m.tasksMutex.Lock()
			for i := range m.WorkerTasks {
				if m.WorkerTasks[i].isExpired() {
					log.Printf("Assuming worker %s died, resetting to Idle the expired task %v", m.WorkerTasks[i].AssignedWorkerName, m.WorkerTasks[i])
					m.WorkerTasks[i].State = Idle
					m.WorkerTasks[i].AssignedWorkerName = ""
					m.WorkerTasks[i].AssignedTimestamp = time.Time{}
				}
			}
			m.tasksMutex.Unlock()

			//lint:ignore SA1015 we are on Go 1.23+, so this is not a leak
			<-time.Tick(time.Second) //nolint SA1015: using time.Tick leaks the underlying ticker
		}
	}()
}

func (m *Master) startRPCServer() {
	if err := rpc.Register(m); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	sock := masterSocket()
	os.Remove(sock)
	listener, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Printf("http.Serve() returned error: %v", err)
		}
	}()
}

func InitMaster(reduceTasks int, inputFilepaths []string) *Master {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix("[master] ")
	m := Master{R: reduceTasks}

	for mapTaskNumber, splitPath := range inputFilepaths {
		m.addTask(WorkerTask{
			Type:           Map,
			State:          Idle,
			TaskNumber:     mapTaskNumber,
			InputFilePaths: []string{splitPath},
		})
	}
	log.Printf("Initialized master with %d mapper tasks", len(m.WorkerTasks))

	m.startRPCServer()
	m.startTasksDaemon()

	return &m
}

func (m *Master) Done() bool {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()

	return m.allTasksFinished()
}
