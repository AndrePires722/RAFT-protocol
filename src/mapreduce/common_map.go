package mapreduce

import (
	"hash/fnv"
	"fmt"
    "io/ioutil"
	"encoding/json"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//Read the file to get contents
	fileContents, _ := ioutil.ReadFile(inFile)
    //check(err)
	
	keyVals := mapF(inFile,string(fileContents))
	
	partition := make([][]KeyValue, nReduce)
	names := make([]string,nReduce)
	
	for _,kv := range keyVals{
		hv := int(ihash(kv.Key))%nReduce
		partition[hv] = append(partition[hv],kv)
	}

	for i:=0;i<nReduce;i++{
		names[i] = reduceName(jobName,mapTaskNumber,i)
	}
	
	//Put data inside the 
	for i:=0;i<nReduce;i++{
		emptyFile, e := os.Create(names[i])
		if e != nil {
			fmt.Printf("MAP ERR: %v\n",e)
		}
		enc := json.NewEncoder(emptyFile)
		
			err := enc.Encode(&partition[i])
			if(err!=nil){
				fmt.Printf("MAP ERR: %v\n",err)
			}
		
		emptyFile.Close()
		
	
		
	}
	
	
	
	
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. 
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}


func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
