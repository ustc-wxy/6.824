package mr

import (
	"fmt"
	"time"
)

type JobMetaInfo struct {
	JobCondition  JobCondition
	StartTime     time.Time
	HeartBeatTime time.Time
	JobPtr        *Job
}

type JobCondition int

const (
	JobRunning = iota
	JobWaiting
	JobDone
)

type JobMetaMap struct {
	MetaMap map[int]*JobMetaInfo
}

func (j *JobMetaMap) get(JobId int) (bool, *JobMetaInfo) {
	res, ok := j.MetaMap[JobId]
	return ok, res
}

func (j *JobMetaMap) put(info *JobMetaInfo) bool {
	jobId := info.JobPtr.JobId
	meta, _ := j.MetaMap[jobId]
	if meta != nil {
		fmt.Println("Put fail! job", jobId, " already exist.")
		return false
	} else {
		j.MetaMap[jobId] = info
	}
	return true
}
func (j *JobMetaMap) checkMapJobState() bool {
	mapDoneNum := 0
	mapUnDoneNum := 0
	for _, meta := range j.MetaMap {
		if meta.JobPtr.JobType == MapJob {
			if meta.JobCondition == JobDone {
				mapDoneNum++
			} else {
				mapUnDoneNum++
				fmt.Println("[mapck]map job", meta.JobPtr.JobId, " is UnDone")
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done\n", mapDoneNum, mapUnDoneNum+mapDoneNum)
	if mapDoneNum > 0 && mapUnDoneNum == 0 {
		return true
	}
	return false
}
func (j *JobMetaMap) checkReduceJobState() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	for _, meta := range j.MetaMap {
		if meta.JobPtr.JobType == ReduceJob {
			if meta.JobCondition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
				fmt.Println("[reduceck]reduce job", meta.JobPtr.JobId, " is UnDone")
			}
		}
	}
	fmt.Printf("%d/%d reduce jobs are done\n", reduceDoneNum, reduceUndoneNum+reduceDoneNum)
	if reduceDoneNum > 0 && reduceUndoneNum == 0 {
		return true
	}
	return false
}
func (j *JobMetaMap) checkAllJobState() bool {
	mapDoneNum := 0
	mapUnDoneNum := 0
	reduceDoneNum := 0
	reduceUnDoneNum := 0

	for _, meta := range j.MetaMap {
		if meta.JobPtr.JobType == MapJob {
			if meta.JobCondition == JobDone {
				mapDoneNum++
			} else {
				mapUnDoneNum++
				fmt.Println("[debug]map job", meta.JobPtr.JobId, " is UnDone")
			}
		} else {
			if meta.JobCondition == JobDone {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
				fmt.Println("[debug]reduce job", meta.JobPtr.JobId, " is UnDone")
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce jobs are done\n",
		mapDoneNum, mapUnDoneNum+mapDoneNum, reduceDoneNum, reduceUnDoneNum+reduceDoneNum)
	if mapDoneNum > 0 && mapUnDoneNum == 0 {
		return true
	}
	if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}
