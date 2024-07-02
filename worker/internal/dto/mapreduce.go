package dto

import "time"

type MapReducer struct {
	LastRun     time.Time `json:"lastRun"`
	InputFiles  []string  `json:"inputFiles"`
	OutputFiles []string  `json:"outputFiles"`
	Status      Status    `json:"status"`
	Task        Task      `json:"task"`
}
