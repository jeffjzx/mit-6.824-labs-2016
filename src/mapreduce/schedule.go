package mapreduce

import (
	"fmt"
	)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	workerName := <-mr.registerChannel
	args := new(DoTaskArgs)
	args.Phase = phase
	args.NumOtherPhase = nios
	args.JobName = mr.jobName


	for i := 0; i < ntasks; i++ {
		args.File = mr.files[i]
		args.TaskNumber = i
		ok := call(workerName, "Worker.DoTask", args, new(struct{}))

		for !ok {
			outerBreak := false
			for j := 0; j < len(mr.workers); j++ {
				ok = call(mr.workers[j], "Worker.DoTask", args, new(struct{}))
				if ok {
					outerBreak = true
					break
				}
			}
			if outerBreak {
				break
			}
		}
	}
	

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
