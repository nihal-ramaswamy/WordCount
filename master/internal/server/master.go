package server

import (
	"encoding/json"
	"log"
	"mapreduce_master/internal/dto"
	"mapreduce_master/internal/utils"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	Files       []string         `json:"files"`
	Mapper      []dto.MapReducer `json:"mapper"`
	Reducer     []dto.MapReducer `json:"reducer"`
	Merger      dto.MapReducer   `json:"merger"`
	Mutex       sync.Mutex       `json:"mutex"`
	NumReducers int              `json:"numReducers"`
	MapperDone  bool             `json:"mapperDone"`
	ReducerDone bool             `json:"reducerDone"`
	MergeDone   bool             `json:"mergeDone"`
}

func InitMaster(files []string, numReducers int, doneCh chan bool) *Master {
	numFiles := len(files)

	m := &Master{
		Files:       files,
		Mapper:      make([]dto.MapReducer, numFiles),
		Reducer:     make([]dto.MapReducer, numReducers),
		Mutex:       sync.Mutex{},
		NumReducers: numReducers,
		MapperDone:  false,
		ReducerDone: false,
		MergeDone:   false,
	}

	for i := range m.Mapper {
		m.Mapper[i] = dto.MapReducer{
			InputFiles:  []string{files[i]},
			OutputFiles: []string{utils.GenerateIntermediateFile(files[i])},
			Task:        dto.Map,
			Status:      dto.Unassigned,
			LastRun:     time.Unix(0, 0),
		}
	}

	numFilesPerReducer := numFiles / numReducers
	remainingFiles := numFiles % numReducers
	lastIdx := -1

	extraFiles := func(val *int) int {
		if *val > 0 {
			*val--
			return 1
		}
		return 0
	}

	for i := range m.Reducer {
		m.Reducer[i] = dto.MapReducer{
			OutputFiles: []string{utils.GenerateOutputPartFile(files[i], i)},
			Task:        dto.Reduce,
			Status:      dto.Unassigned,
			LastRun:     time.Unix(0, 0),
		}
		filesToAdd := numFilesPerReducer + extraFiles(&remainingFiles)
		for j := 0; j < filesToAdd; j++ {
			m.Reducer[i].InputFiles = append(m.Reducer[i].InputFiles, utils.GenerateIntermediateFile(files[lastIdx+1]))
			lastIdx++
		}
	}

	m.Merger = dto.MapReducer{
		Task:        dto.Merge,
		Status:      dto.Unassigned,
		InputFiles:  []string{},
		OutputFiles: []string{"final.txt"},
		LastRun:     time.Unix(0, 0),
	}

	for i := range numReducers {
		m.Merger.InputFiles = append(m.Merger.InputFiles, m.Reducer[i].OutputFiles...)
	}

	temp, _ := json.Marshal(m)
	log.Printf("Master: %v", string(temp))

	m.startServer(doneCh)

	return m
}

func (m *Master) startServer(doneCh chan bool) {
	rpc.Register(m)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", "localhost:1234")
	if nil != err {
		log.Fatal(err)
	}

	go http.Serve(listener, nil)

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			m.done(doneCh)
		case done := <-doneCh:
			if done {
				ticker.Stop()
				return
			}
		}
	}
}

func (m *Master) setMapper(index int, lastRun time.Time, status dto.Status) {
	m.Mapper[index].Status = status
	m.Mapper[index].LastRun = lastRun
}

func (m *Master) setReducer(index int, lastRun time.Time, status dto.Status) {
	m.Reducer[index].Status = status
	m.Reducer[index].LastRun = lastRun
}

func (m *Master) setReply(mapReducer dto.MapReducer, index int, status dto.Status, reply *dto.Reply) {
	reply.Task = mapReducer
	reply.Index = index
	reply.Task.Status = status
}

func (m *Master) done(doneCh chan bool) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	doneCh <- m.MapperDone && m.ReducerDone && m.MergeDone
}

func (m *Master) GetTask(args *dto.Args, reply *dto.Reply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	a, _ := json.Marshal(args)
	log.Printf("GetTask Args: %v", string(a))

	countDone := 0
	for i := range m.Mapper {
		if dto.Done == m.Mapper[i].Status {
			countDone++
			continue
		}
		if dto.Unassigned == m.Mapper[i].Status || time.Since(m.Mapper[i].LastRun) > time.Minute {
			m.setMapper(i, time.Now(), dto.InProgress)
			m.setReply(m.Mapper[i], i, dto.InProgress, reply)
			log.Printf("Reply %v", reply)

			return nil
		}
	}

	if countDone == len(m.Mapper) {
		m.MapperDone = true
	} else {
		reply.Task.Task = dto.Wait
		return nil
	}

	countDone = 0

	for i := range m.Reducer {
		if dto.Done == m.Reducer[i].Status {
			countDone++
			continue
		}
		if dto.Unassigned == m.Reducer[i].Status || time.Since(m.Reducer[i].LastRun) > time.Minute {
			m.setReducer(i, time.Now(), dto.InProgress)
			m.setReply(m.Reducer[i], i, dto.InProgress, reply)
			log.Printf("Reply %v", reply)
			return nil
		}
	}

	if countDone == len(m.Reducer) {
		m.ReducerDone = true
	} else {
		reply.Task.Task = dto.Wait
		return nil
	}

	if !m.MergeDone {
		reply.Task = m.Merger
		reply.Task.Status = dto.InProgress

		log.Printf("Reply %v", reply)

		return nil
	}

	reply.Task.Task = dto.Exit

	return nil
}

func (m *Master) NotifyTaskDone(args *dto.Reply, reply *dto.Reply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	task := args.Task.Task

	r, _ := json.Marshal(args)
	log.Printf("NotifyTaskDone: %v", string(r))

	reply.Task.Status = dto.Done

	if dto.Map == task {
		m.Mapper[args.Index].Status = dto.Done
	} else if dto.Reduce == task {
		m.Reducer[args.Index].Status = dto.Done
	} else if dto.Merge == task {
		m.MergeDone = true
	}

	return nil
}
