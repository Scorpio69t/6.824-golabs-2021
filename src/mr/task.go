package mr

type MRTask struct {
	Type   string // "Map" or "Reduce"
	Number int    // Task Number
	File   string // File name
	Status string // "NotStart", "Running" or "Finished"
}

const (
	NotStart = "NotStart"
	Running = "Running"
	Finished = "Finished"
	MapType = "Map"
	ReduceType = "Reduce"
)