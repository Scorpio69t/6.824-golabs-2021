package mr

type MRTask struct {
	Type   uint // "Map" or "Reduce"
	Number int    // Task Number
	File   string // File name
	Status uint // "NotStart", "Running" or "Finished"
}

const (
	NotStart = iota
	Running = iota
	Finished = iota
)

const (
	MapType = iota
	ReduceType = iota
)