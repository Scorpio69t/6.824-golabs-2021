package mr

type TaskStatus uint
type TaskType uint

type MRTask struct {
	Type   TaskType   // "Map" or "Reduce"
	Number int        // Task Number
	File   string     // File name
	Status TaskStatus // "NotStart", "Running" or "Finished"
}

const (
	StatusNotStart = iota
	StatusRunning  = iota
	StatusFinished = iota
)

const (
	MapType    = iota
	ReduceType = iota
)

func ConvertTaskTypeToString(t TaskType) string {
	switch t {
	case MapType:
		return "Map"
	case ReduceType:
		return "Reduce"
	}

	return "Unknown"
}

func ConvertTaskStatusToString(s TaskStatus) string {
	switch s {
	case StatusNotStart:
		return "NotStart"
	case StatusRunning:
		return "Running"
	case StatusFinished:
		return "StatusFinished"
	}

	return ""
}
