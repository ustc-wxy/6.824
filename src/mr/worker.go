package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	reducef func(string, []string) string, workerId int) {
	// Your worker implementation here.
	fmt.Println("worker id is", workerId)
	alive := true
	attempt := 0
	for alive {
		attempt++
		fmt.Println("worker", workerId, " attempts ", attempt, "times")

		job := CallAssignJob()
		switch job.JobType {
		case MapJob:
			{
				DoMap(mapf, job, workerId)
				fmt.Println("worker", workerId, " do map job", job.JobId, " successful!")
				CallSetJobDone(job)
			}
		case ReduceJob:
			{
				DoReduce(reducef, job)
				fmt.Println("worker", workerId, " do reduce job", job.JobId, " successful!")
				CallSetJobDone(job)
			}
		case WaitJob:
			{
				fmt.Println("wait a moment~")
				time.Sleep(time.Second)
			}
		case KillJob:
			{
				res := CallCheckAllDone()
				if *res {
					fmt.Println("worker", workerId, " terminated...")
					time.Sleep(time.Second)
					alive = false
				} else {
					fmt.Println("worker", workerId, " wait for exit")
					time.Sleep(time.Second)
				}

			}
		}
		time.Sleep(time.Second)
	}

}
func DoMap(mapf func(string, string) []KeyValue, job *Job, workerId int) error {
	go func() {
		for {
			res := *CallGetJobState(job.JobId)
			//fmt.Println("[debug],res is", res)
			if res {
				SendHeartBeat(job.JobId)
			} else {
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	var intermediate []KeyValue
	filename := job.InputFiles[0]

	f, e := os.Open(filename)
	if e != nil {
		fmt.Errorf("cannot open %v", filename)
		return e
	}
	//fmt.Println("Domap debug0 id is", workerId)
	c, e := ioutil.ReadAll(f)
	if e != nil {
		return e
	}
	f.Close()
	intermediate = mapf(filename, string(c))

	rn := job.ReduceNum
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % rn
		HashedKV[idx] = append(HashedKV[idx], kv)
	}

	for i := 0; i < rn; i++ {
		tmpname := "mr-tmp-" + strconv.Itoa(job.JobId) + "-" + strconv.Itoa(i)
		tmpfile, _ := os.Create(tmpname)
		enc := json.NewEncoder(tmpfile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		tmpfile.Close()
	}

	return nil
}
func DoReduce(reducef func(string, []string) string, job *Job) {
	go func() {
		for {
			res := *CallGetJobState(job.JobId)
			//fmt.Println("[debug],res is", res)
			if res {
				SendHeartBeat(job.JobId)
			} else {
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	intermediate := readLocalFiles(job.InputFiles)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	jobid := job.JobId
	filename := fmt.Sprintf(dir+"/mr-out-%d", jobid)
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	defer tempFile.Close()
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tempFile.Name(), filename)
}
func readLocalFiles(files []string) []KeyValue {
	var kvList []KeyValue
	for _, filename := range files {
		f, _ := os.Open(filename)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			e := dec.Decode(&kv)
			if e != nil {
				break
			}
			kvList = append(kvList, kv)
		}
		f.Close()
	}
	return kvList
}
func SendHeartBeat(jobId int) {
	args := jobId
	reply := &EmptyReply{}
	call("Coordinator.ReceiveHeartBeat", &args, &reply)
	fmt.Println(jobId, " send hb signal success!")
}
func CallGetJobState(jobId int) *bool {

	var res bool
	args := jobId
	reply := &res
	call("Coordinator.GetJobState", &args, &reply)
	return &res
}
func CallSetJobDone(job *Job) {
	args := job
	reply := &EmptyReply{}
	call("Coordinator.SetJobDone", &args, &reply)
}
func CallCheckAllDone() *bool {
	var res bool
	args := &EmptyArgs{}
	reply := &res
	call("Coordinator.CheckAllDone", &args, &reply)
	return &res
}
func CallAssignJob() *Job {
	args := AssignJobArgs{}
	reply := Job{}
	ok := call("Coordinator.AssignJob", &args, &reply)
	if ok {
		fmt.Printf("reply.job %v\n", reply)
	} else {
		fmt.Printf("call assign job failed!\n")
	}
	return &reply
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
