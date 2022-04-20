package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

//struct of coordinator
type Coordinator struct {
	MapJobChannel        chan *Job
	ReduceJobChannel     chan *Job
	MapNum               int
	ReduceNum            int
	uniqueIdCounter      int
	CoordinatorCondition CoordinatorCondition
	jobMetaMap           JobMetaMap
}

//struct of job
type Job struct {
	JobType    JobType
	InputFiles []string
	JobId      int
	ReduceNum  int
}

//type of job
type JobType int

const (
	MapJob = iota
	ReduceJob
	WaitJob
	KillJob
)

//condition of coordinator
type CoordinatorCondition int

const (
	MapPhasing = iota
	ReducePhasing
	AllDone
)

//metaInfo describe the basic info about Job

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhasing {
		c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhasing
	} else if c.CoordinatorCondition == ReducePhasing {
		c.CoordinatorCondition = AllDone
	}
}
func (c *Coordinator) CheckAllDone(args *EmptyArgs, reply *bool) error {
	mu.Lock()
	res := c.jobMetaMap.checkAllJobState()
	*reply = res
	mu.Unlock()
	return nil
}
func (c *Coordinator) GetJobState(args int, reply *bool) error {
	mu.Lock()
	jobId := args

	_, meta := c.jobMetaMap.get(jobId)
	//fmt.Println("[debug],meta is", meta)
	var res bool
	if meta.JobCondition == JobRunning {
		res = true
	} else {
		res = false
	}
	//fmt.Println("res is ", res)
	*reply = res
	mu.Unlock()
	return nil
}
func (c *Coordinator) ReceiveHeartBeat(args int, reply *EmptyReply) error {
	jobId := args
	mu.Lock()
	ok, meta := c.jobMetaMap.get(jobId)
	if ok && meta.JobCondition == JobRunning {
		meta.HeartBeatTime = time.Now()
		fmt.Println("job", jobId, " receive heartbeat.")
	} else {
		fmt.Println("[error]HeartBeat failed!")
	}
	mu.Unlock()
	return nil

}
func (c *Coordinator) AssignJob(args *AssignJobArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("Assigning...")
	if c.CoordinatorCondition == MapPhasing {
		if len(c.MapJobChannel) > 0 {
			*reply = *<-c.MapJobChannel
			if !c.StartJob(reply.JobId) {
				fmt.Println("[error] Start map job", reply.JobId, " fail!")
			}
		} else {
			reply.JobType = WaitJob
			if c.jobMetaMap.checkMapJobState() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhasing {
		if len(c.ReduceJobChannel) > 0 {
			*reply = *<-c.ReduceJobChannel
			if !c.StartJob(reply.JobId) {
				fmt.Println("[error] Start map job", reply.JobId, " fail!")
			}
		} else {
			reply.JobType = WaitJob
			if c.jobMetaMap.checkReduceJobState() {
				c.nextPhase()
			}
		}
	} else {
		reply.JobType = KillJob
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.gnerateJobId()
		job := Job{
			JobType:    MapJob,
			InputFiles: []string{v},
			JobId:      id,
			ReduceNum:  c.ReduceNum,
		}
		meta := JobMetaInfo{
			JobCondition: JobWaiting,
			JobPtr:       &job,
		}
		fmt.Println("making map job:", &job)
		c.jobMetaMap.put(&meta)
		c.MapJobChannel <- &job
	}
	fmt.Println("make map job finished!")
}
func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.gnerateJobId()
		job := Job{
			JobType:    ReduceJob,
			JobId:      id,
			InputFiles: TmpFileAssginHelper(i),
			ReduceNum:  c.ReduceNum,
		}
		meta := JobMetaInfo{
			JobCondition: JobWaiting,
			JobPtr:       &job,
		}
		fmt.Println("making reduce job:", &job)
		c.jobMetaMap.put(&meta)
		c.ReduceJobChannel <- &job
	}
	fmt.Println("make reduce job finished!")
}
func TmpFileAssginHelper(IndexReduce int) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(IndexReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func (c *Coordinator) gnerateJobId() int {
	curId := c.uniqueIdCounter
	c.uniqueIdCounter++
	return curId
}

func (c *Coordinator) StartJob(jobId int) bool {
	ok, meta := c.jobMetaMap.get(jobId)
	if ok && meta.JobCondition == JobWaiting {
		meta.JobCondition = JobRunning
		meta.StartTime = time.Now()
		meta.HeartBeatTime = time.Now()
		if meta.JobPtr.JobType == MapJob {
			fmt.Println("Map job", jobId, " start...")
		} else if meta.JobPtr.JobType == ReduceJob {
			fmt.Println("Reduce job", jobId, " start...")
		}
	} else {
		return false
	}
	return true
}
func (c *Coordinator) SetJobDone(args *Job, reply *EmptyReply) error {
	mu.Lock()
	defer mu.Unlock()
	ok, meta := c.jobMetaMap.get(args.JobId)
	if ok && meta.JobCondition == JobRunning {
		meta.JobCondition = JobDone
		if args.JobType == MapJob {
			fmt.Println("Map job", args.JobId, " is completed")
		} else if args.JobType == ReduceJob {
			fmt.Println("Reduce job", args.JobId, " is completed")
		}
	} else {
		//fmt.Println("[error] Fail to set job", args.JobId, "  to done!")
		panic("run job done")
	}
	return nil
}
func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorCondition == AllDone {
			mu.Unlock()
			return
		}

		timenow := time.Now()
		for _, v := range c.jobMetaMap.MetaMap {
			//fmt.Println(v)
			if v.JobCondition == JobRunning {
				fmt.Println("job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
				fmt.Println("heartbeating for", timenow.Sub(v.HeartBeatTime))
			}

			if v.JobCondition == JobRunning && time.Now().Sub(v.HeartBeatTime) > 10*time.Second {
				fmt.Println("detect a crash on job ", v.JobPtr.JobId)
				switch v.JobPtr.JobType {
				case MapJob:
					c.MapJobChannel <- v.JobPtr
					v.JobCondition = JobWaiting
				case ReduceJob:
					c.ReduceJobChannel <- v.JobPtr
					v.JobCondition = JobWaiting

				}
			}
		}
		mu.Unlock()
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapJobChannel:        make(chan *Job, len(files)),
		ReduceJobChannel:     make(chan *Job, nReduce),
		MapNum:               len(files),
		ReduceNum:            nReduce,
		CoordinatorCondition: MapPhasing,
		uniqueIdCounter:      0,
		jobMetaMap: JobMetaMap{
			MetaMap: make(map[int]*JobMetaInfo),
		},
	}

	c.makeMapJobs(files)
	// Your code here.

	c.server()
	go c.CrashHandler()
	return &c
}
