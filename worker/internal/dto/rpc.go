package dto

type Args struct {
	Task   MapReducer `json:"task"`
	TaskNo int        `json:"taskNo"`
}

type Reply struct {
	Task  MapReducer `json:"task"`
	Index int        `json:"index"`
}
