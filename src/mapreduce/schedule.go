package mapreduce

import (
	
	"fmt"
	"sync"
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
	
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	
	var lock sync.Mutex
	tasks := make([]int,ntasks)
	for i,_ := range tasks{
			tasks[i] = i
	}
	
	var statLock sync.Mutex
	status := make(map[string]bool)
	
	
	for true{
		
		for true{
			worker := <- mr.registerChannel
			//fmt.Printf("Worker %v REPORTING FOR DUTY\n",worker)
			lock.Lock()
			if(len(tasks)>0){
				
				task := tasks[0]
				tasks = tasks[1:]
				lock.Unlock()
				statLock.Lock()
				status[worker] = true
				statLock.Unlock()
				go func(i int){
					//fmt.Printf("TASK NUMBER %v being given to %v\n",i,worker)
					ch := ShutdownReply{}
					args := DoTaskArgs{mr.jobName,mr.files[i],phase,i,nios}
					result := call(worker, "Worker.DoTask", args, &ch)
					if(!result){
						//fmt.Println("DOTASK FAILED")
						lock.Lock()
						tasks = append(tasks,i)
						//fmt.Printf("Returned task #%v, remaining tasks: %v\n",i,len(tasks))
						lock.Unlock()
						
						statLock.Lock()
						status[worker] = false
						statLock.Unlock()
					
					}else{
						
					}
					
					register := RegisterArgs{worker}
					for true {
						//fmt.Printf("WORKER : %v attempting to register\n",worker)
						result = call(mr.address,"Master.Register",register,&ch)
						if(result){break}
					}
					
					if(!result){
						//fmt.Println("REG FAILED")
					}else{
						//fmt.Println("REG SUCC")
					}
					statLock.Lock()
					status[worker] = false
					statLock.Unlock()
				}(task)
				
			}else{
				lock.Unlock()
				ch := ShutdownReply{}
				register := RegisterArgs{worker}
				for true {
					result := call(mr.address,"Master.Register",register,&ch)
					if(result){break}
				}
				break
			}
		}
		
		for k,_ := range status {
			fmt.Printf("checking status of worker %v\n",k)
			for status[k] {}
			fmt.Printf("worker %v confirmed done\n",k)
		}
		
		lock.Lock()
		if(len(tasks)>0){
			fmt.Printf("TASKS ACTUALLY NOT DONE : remaining %v\n", len(tasks))
			lock.Unlock()
			
		}else{
			fmt.Println("TASKS DONE")
			lock.Unlock()
			break
		}
		
	}
	
	return
}


