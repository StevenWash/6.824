package mr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


type Worker struct {
	WorkerId string
	MasterId string
	Aksk string

	ProcessTask Task

	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

var worker *Worker

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// do map task
func mapTask() error {

	task := worker.ProcessTask
	intermediate :=  []KeyValue{}
	filename := task.FileName

	if filename == "" || len(filename) == 0 {
		return errors.New("Error for file name of a task")
	}

	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()


	kva := worker.mapf(filename, string(content))

	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	idx := ihash(filename) % task.NReduce
	outFileName := "tmp.intermediate." + strconv.Itoa(idx)

	tmpFile, err := os.Create(outFileName + ".temp")
	if err != nil {
		log.Fatalf("cannot open %v", tmpFile)
		return err
	}
	enc := json.NewEncoder(tmpFile)
	for _, kv := range intermediate {
		err := enc.Encode(kv)
		if err != nil {
			log.Fatalf("cannot encode %s to json", kv)
			return err
		}
	}

	data, err := ioutil.ReadFile(tmpFile.Name())
	if err != nil {
		log.Fatalf("read file error: %s", err.Error())
		return err
	}

	var outFile *os.File
	//(outFileName)
	//if errF != nil {_, errF := os.Stat
	outFile, err = os.OpenFile(outFileName, os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("cannot open %v", outFile)
		return err
	}

	err = ioutil.WriteFile(outFile.Name(), data,0666 )
	if err != nil {
		log.Fatalf("IO copy error %s", err.Error())
		return err
	}
	//}

	//err = os.Remove(tmpFile.Name())
	//if err != nil {
	//	log.Fatalf("Remove tmp file error: %s", err.Error())
	//	return err
	//}

	worker.ProcessTask.FileName = outFileName
	worker.ProcessTask.ModifyTime = time.Now()

	defer tmpFile.Close()
	defer outFile.Close()

	return nil
}

// do reduce task
func reduceTask()  error{

	task := worker.ProcessTask
	intermediate :=  []KeyValue{}
	filename := task.FileName

	if filename == "" || len(filename) == 0 {
		return errors.New("Error for file name of a task")
	}

	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	reduceIdx := strings.Split(task.FileName, ".")[2]

	oname := "mr-out-"+reduceIdx
	//ofile, _ := os.Create(oname)


	var outFile *os.File

	outFile, err = os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("cannot open %v", outFile)
		return err
	}

	writer := bufio.NewWriter(outFile)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := worker.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

		data := fmt.Sprintf("%v %v\n", intermediate[i].Key, output)

		_, err := writer.WriteString(data)

		//err = ioutil.WriteFile(outFile.Name(), []byte(data),0666 )
		if err != nil {
			log.Fatalf("Write to file  error %s", err.Error())
			return err
		}

		i = j
	}

	writer.Flush()


	//err = os.Remove(task.FileName)
	//if err != nil {
	//	log.Fatalf("Remove tmp file error: %s", err.Error())
	//	return err
	//}

	worker.ProcessTask.FileName = oname
	worker.ProcessTask.ModifyTime = time.Now()

	defer outFile.Close()

	return nil
}

func askForTask()  {
	//  send the RPC request to the master for a Task.
	reply := worker.CallTask()

	//log.Printf("Woker get reply from master: %v \n", reply)
	task := reply.Task

	if task.TaskID == "" || len(task.TaskID) == 0 {
		log.Fatalf("cannot get a task")
		return
	}

	worker.ProcessTask = task

	// 根据任务的类型执行不同的操作：map或reduce
	if task.TaskType == MAP_TYPE {
		worker.ProcessTask.Status = MAPPING
		err := mapTask()
		if err != nil {
			log.Fatalf("mapTask run error: %v", err.Error())
			worker.CallTaskResp(MAPFAIL)
			return
		}
		worker.CallTaskResp(MAPOK)
	} else if task.TaskType == REDUCE_TYPE {
		worker.ProcessTask.Status = REDUCING
		err := reduceTask()
		if err != nil {
			log.Fatalf("reduceTask run error: %v", err.Error())
			worker.CallTaskResp(REDUCEFAIL)
			return
		}
		worker.CallTaskResp(REDUCEOK)
	} else {
		log.Fatalln("Not support task type: " + strconv.Itoa(int(task.TaskType)))
		worker.CallTaskResp(ERROR)
		return
	}

}

//
// main/mrworker.go calls this function.
//
func Work(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// 实例化一个worker对象，并注册到Master中
	worker = &Worker{
		WorkerId: GeneratUUID(),
		MasterId: "",
		Aksk:     AKSK,
	}

	worker.mapf = mapf
	worker.reducef = reducef

	ret := worker.CallRegister()
	if !ret.Status {
		log.Fatalf("Error for worker %s register to master\n", worker.WorkerId)
		return
	}

	//log.Printf("Worker %s register to master %s success :[%v]!", ret.Worker.WorkerId, ret.Worker.MasterId, ret)
	worker.MasterId = ret.Worker.MasterId

	if worker.WorkerId != ret.Worker.WorkerId {
		log.Fatalf("Error for get worker:%s info from master: %s", worker.WorkerId, ret.Worker.WorkerId)
		return
	}

	for {
		askForTask()
		time.Sleep(2*time.Second)
	}

	return
}


func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (w *Worker)CallTask() TaskReply{

	args := TaskArgs{
		WorkerID: w.WorkerId,
		MasterID: w.MasterId,
	}

	reply := TaskReply{}

	err := call("Master.Task", &args, &reply)
	if !err {
		log.Fatalf("cannot call for a task")
	}

	//log.Printf("Woker get reply from master: %v \n", reply)

	return reply
}

func (w *Worker)CallTaskResp(taskStatus StatusType) TaskReply{

	w.ProcessTask.Status = taskStatus

	args := TaskArgs{
		WorkerID: w.WorkerId,
		MasterID: w.MasterId,

		Task: w.ProcessTask,
	}

	reply := TaskReply{}

	err := call("Master.TaskResp", &args, &reply)
	if !err {
		log.Fatalf("cannot call resp to master")
	}

	return reply
}

// Register worker to master
func (w *Worker)CallRegister() RegisterWorker {
	args := RegisterWorker{
		Worker: Worker{
			WorkerId:    w.WorkerId,
			MasterId:    "",
			Aksk:        w.Aksk,
			ProcessTask: Task{},
		},
		Status:   false,
	}

	reply := RegisterWorker{}


	ret := call("Master.Register", &args, & reply)
	if !ret {
		log.Fatalln("CallRegister: register failed!")
	}

	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err.Error())
	return false
}
