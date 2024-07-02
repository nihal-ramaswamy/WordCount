package dto

import (
	"encoding/json"
	"fmt"
)

type (
	Status int
	Task   int
)

// Status
const (
	Unassigned Status = iota
	InProgress
	Done
)

var StatusToString = map[Status]string{
	Unassigned: "Unassigned",
	InProgress: "InProgress",
	Done:       "Done",
}

var StringToStatus = map[string]Status{
	"Unassigned": Unassigned,
	"InProgress": InProgress,
	"Done":       Done,
}

func (s Status) MarshallJSON() ([]byte, error) {
	if str, ok := StatusToString[s]; ok {
		return json.Marshal(str)
	}

	return nil, fmt.Errorf("unknown status type %d", s)
}

func (s *Status) UnmarshalJSON(text []byte) error {
	var str string
	if err := json.Unmarshal(text, &str); err != nil {
		return err
	}
	var v Status
	var ok bool
	if v, ok = StringToStatus[str]; !ok {
		return fmt.Errorf("unknown user type %s", str)
	}
	*s = v
	return nil
}

// Task
const (
	Map Task = iota
	Reduce
	Merge
	Wait
	Exit
)

var TaskToString = map[Task]string{
	Map:    "Map",
	Reduce: "Reduce",
	Merge:  "Merge",
	Wait:   "Wait",
	Exit:   "Exit",
}

var StringToTask = map[string]Task{
	"Map":    Map,
	"Reduce": Reduce,
	"Merge":  Merge,
	"Wait":   Wait,
	"Exit":   Exit,
}

func (s Task) MarshallJSON() ([]byte, error) {
	if str, ok := TaskToString[s]; ok {
		return json.Marshal(str)
	}

	return nil, fmt.Errorf("unknown status type %d", s)
}

func (s *Task) UnmarshalJSON(text []byte) error {
	var str string
	if err := json.Unmarshal(text, &str); err != nil {
		return err
	}
	var v Task
	var ok bool
	if v, ok = StringToTask[str]; !ok {
		return fmt.Errorf("unknown user type %s", str)
	}
	*s = v
	return nil
}
