package mr

import (
	"errors"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	TaskList map[string]Task

	WorkerList map[string]Worker

	Aksk string
	MasterID string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Task(args *TaskArgs, reply *TaskReply) error {
	//log.Printf("Master Task get args: %v \n", args)

	if m.MasterID != args.MasterID {
		return errors.New("master id not correct!")
	}

	//log.Printf("Master Worker List: %v \n", m.WorkerList)

	workerId := args.WorkerID
	if _, ok := m.WorkerList[workerId]; ok {

		reply.MasterID = m.MasterID
		reply.WorkerID = workerId

		// 返回task: INIT->MAPOK->REDUCE
		// 需要实现任务状态机
		for _, task := range m.TaskList {
			if task.Status == INIT {

				log.Println("find task：INIT")
				reply.Task = task
				return nil
			}
		}

		for _, task := range m.TaskList {
			if task.Status == MAPOK {

				log.Println("find task: MPAOK")
				reply.Task = task
				return nil
			}
		}
	}

	//log.Println("find no task")
	return nil
}

func (m *Master) TaskResp(args *TaskArgs, reply *TaskReply) error {
	//log.Printf("Master TaskResp get args: %v \n", args)

	if m.MasterID != args.MasterID {
		return errors.New("master id not correct!")
	}

	workerId := args.WorkerID

	if _, ok := m.WorkerList[workerId]; !ok {
		log.Fatalf("worker id not correct")
		return errors.New("worker id not correct!")
	}

	task := args.Task
	if task.Status == MAPPING || task.Status == MAPFAIL {
		task.Status = INIT
		task.TaskType = MAP_TYPE
	}

	if task.Status == REDUCING || task.Status == REDUCEFAIL {
		task.Status = MAPOK
		task.TaskType = REDUCE_TYPE
	}

	if task.Status == MAPOK {
		task.TaskType = REDUCE_TYPE
	}

	if task.Status == REDUCEOK {
		task.Status = COMPLETED
	}

	if _, ok := m.TaskList[task.TaskID]; ok {
		m.TaskList[task.TaskID] = task
	} else {
		log.Fatalf("task not exist")
	}

	return nil
}

func (m *Master) Register(args *RegisterWorker, reply *RegisterWorker) error {

	worker := args.Worker
	//log.Printf("worker %s register!", worker.WorkerId)

	// 验证worker的aksk是否是该master
	if worker.Aksk == m.Aksk {
		m.WorkerList[worker.WorkerId] = worker

		reply.Status = true
	} else {
		reply.Status = false
	}

	reply.Worker = worker
	reply.Worker.MasterId = m.MasterID


	//log.Printf("worker reply: %v", reply)

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	//go func() {
	//	for {
	//		time.Sleep(2*time.Second)
	//		log.Printf("Master: {\n  [TaskList: \n  %v]\n  [WorkerList:\n %v]\n  [MasterID: %s]\n}", m.TaskList, m.WorkerList, m.MasterID)
	//	}
	//}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.

	for _, task := range m.TaskList {
		if task.Status != COMPLETED {
			return false
		}
	}

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Aksk: AKSK,
		MasterID: GeneratUUID(),
		TaskList: make(map[string]Task),
		WorkerList: make(map[string]Worker),
	}

	// Your code here.
	for _, file := range files {
		task := Task{
			TaskID: GeneratUUID(),
			FileName:file,
			Status : INIT,
			TaskType: MAP_TYPE,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			NReduce: nReduce,
		}
		m.TaskList[task.TaskID] = task
	}

	//log.Printf("Master struct: %v \n", m)

	m.server()
	return &m
}
