package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			retry_cnt := 0
			for {
				worker := <-registerChan
				debug("ntasks:%v index: %v\n", ntasks, index)
				debug("worker address: %v\n", worker)
				ok := call(worker, "Worker.DoTask", &DoTaskArgs{jobName, mapFiles[index], phase, index, n_other}, nil)
				// go func() { registerChan <- worker }() // 不管worker成功失败都重新放回channel
				if !ok {
					retry_cnt++
					log.Println("rpc call failed, retry cnt: ", retry_cnt)
					continue
				}
				// 只能在go routine里进行操作，否则最后两个task会阻塞在这里
				// 因为registerChan容量是1, 而worker数量是2
				// registerChan <- worker
				go func() { registerChan <- worker }()
				debug("do task %v succ\n", index)
				break

			}

		}(i)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
