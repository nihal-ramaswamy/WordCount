package dto

import "time"

type MapReducer struct {
	LastRun     time.Time `json:"lastRun"`
	InputFiles  []string  `json:"Inputfiles"`
	OutputFiles []string  `json:"outputfiles"`
	Status      Status    `json:"status"`
	Task        Task      `json:"task"`
}
