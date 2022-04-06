package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	workerNum := CallHello()
	fmt.Println(workerNum)
	finished, fileName, nReducer, jobId := CallMapTask(workerNum)
	fmt.Println(finished, fileName, jobId)

	if !finished && fileName != "" {
		reduceFiles, ok := mapTask(fileName, nReducer, jobId, mapf)
		fmt.Println(reduceFiles, ok)
		CallFinishMap(reduceFiles, ok, workerNum, jobId)
	}

	time.Sleep(1 * time.Hour)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Construct RPC Connection
func CallHello() int {
	args := HelloArgs{}
	args.X = "hongjiahao"
	reply := HelloReply{}
	ok := call("Coordinator.Hello", &args, &reply)
	for !ok {
		ok = call("Coordinator.Hello", &args, &reply)
	}
	return reply.Y
}

// Try to Get the Map Task, if the Map Stage have Finished, will recieve the signal to break and turn to CallReduceTask
func CallMapTask(workerNum int) (bool, string, int, int) {
	args := MapArgs{}
	args.WorkerNum = workerNum
	reply := MapReply{}
	ok := call("Coordinator.MapTask", &args, &reply)
	if ok && reply.FileName != "" {
		return false, reply.FileName, reply.NReducer, reply.JobId
	}

	for ok && !reply.Finished {
		time.Sleep(1 * time.Second)
		ok = call("Coordinator.MapTask", &args, &reply)
		if ok && reply.FileName != "" {
			return false, reply.FileName, reply.NReducer, reply.JobId
		}
	}

	return true, "", reply.NReducer, reply.JobId
}

// CallFinishMap: Reply to Coordinator Whether Finish the Map Task or not, if Finish the Task, will Turn to Exe CallReduceTask
// If cannot Finish the Map Task in Resonable Time(10 seconds), will not Get the Channel and Turn to Exe CallMapTask
func CallFinishMap(files []string, f bool, workerNum int, jobId int) {
	args := FinishMapArgs{}
	args.X = f
	args.WorkerNum = workerNum
	args.FileName = files
	args.JobId = jobId
	reply := FinishMapReply{}
	ok := call("Coordinator.FinishMap", &args, &reply)
	for !ok {
		ok = call("Coordinator.FinishMap", &args, &reply)
	}

	return
}

// CallReduceTask: Try to get reduce task until get one or the whole work finished
func CallReduceTask(workerNum int) (bool, []string) {
	args := ReduceTaskArgs{}
	args.WorkerNum = workerNum
	reply := ReduceTaskReply{}
	ok := call("Coordinator.ReduceTask", &args, &reply)
	if ok && reply.ReducerFile != nil && !reply.Finished {
		return false, reply.ReducerFile
	}

	for ok && reply.ReducerFile == nil && !reply.Finished {
		ok = call("Coordinator.ReduceTask", &args, &reply)
	}

	if reply.Finished {
		return true, nil
	}
	return false, reply.ReducerFile
}

// CallFinishReduce: Reply to coordinator whether finish reduce task or not, after that, worker turn to CallReduceTask again
func CallFinishReduce(fileName string, f bool, workerNum int) {
	args := FinishReduceArgs{}
	args.X = f
	args.WorkerNum = workerNum
	args.File = fileName
	reply := FinishReduceReply{}
	ok := call("Coordinator.FinishReduce", &args, &reply)
	for !ok {
		ok = call("Coordinator.FinishReduce", &args, &reply)
	}

	return
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
func mapTask(filename string, nReducer int, jobId int, mapf func(string, string) []KeyValue) ([]string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	intermediate := [][]KeyValue{}
	for i := 0; i < nReducer; i++ {
		t := []KeyValue{}
		intermediate = append(intermediate, t)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	err = os.Mkdir("mapTask"+strconv.Itoa(jobId), os.ModePerm)
	content, err := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(filename, string(content))
	for i := 0; i < len(kva); i++ {
		index := ihash(kva[i].Key) % 10
		intermediate[index] = append(intermediate[index], kva[i])
	}

	for i := 0; i < nReducer; i++ {
		s := "mapTmp-" + strconv.Itoa(jobId) + "-" + strconv.Itoa(i) + "-"
		tmpfile, err := ioutil.TempFile("./", s)
		defer os.Remove(tmpfile.Name())
		if err != nil {
			log.Println(err)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range intermediate[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Println(err)
			}
		}
	}

	reduceFiles := []string{}
	select {
	case <-ctx.Done():
		os.RemoveAll("mapTask" + strconv.Itoa(jobId))
		return nil, false
	default:
		tmpFiles, err := filepath.Glob("mapTmp-" + strconv.Itoa(jobId) + "*")
		if err != nil {
			log.Println(err)
		}
		for i := 0; i < len(tmpFiles); i++ {
			str := strings.Split(tmpFiles[i], "-")
			newName := "mr-" + strconv.Itoa(jobId) + "-" + str[2]
			err = os.Rename(tmpFiles[i], "mapTask"+strconv.Itoa(jobId)+"/"+newName)
			reduceFiles = append(reduceFiles, "mapTask"+strconv.Itoa(jobId)+"/"+newName)
		}
	}

	return reduceFiles, true
}
