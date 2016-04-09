package mapreduce

import (
	"hash/fnv"
	"os"
	"log"
	"encoding/json"
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
	//1.Read the input file
	//2.Call mapF split the input file into Key/Value pairs
	//3.Iterate the Key/Value pairs nReduce time
	//   3.1 Create nReduce intermediate files
	//   3.2 Hash the Key to get the corrsponding file
	//   3.3 Serialize the Key/Value pairs into corresponding intermediate file by json
	file, err := os.Open(inFile)
	debug("DEBUG[doMap]: JobName:%s, mapTaskNumber:%d, File:%s, reduceTask:%d\n", 
			jobName, mapTaskNumber, inFile, nReduce);
	if err != nil {
		log.Fatal("ERROR[doMap]: Open file error ", err);
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("ERROR[doMap]: Get file state error ", err);
	}
	fileSize := fileInfo.Size()
	buf := make([]byte, fileSize)
	_, err = file.Read(buf)
	debug("DEBUG[doMap]: Read from %s %dbyte\n", inFile, fileSize)
	if err != nil {
		log.Fatal("ERROR[doMap]:Read error ", err)
	}
	middle_res := mapF(inFile, string(buf))
	rSize := len(middle_res)
	debug("DEBUG[doMap]: Map result pair size %d\n", rSize)
	file.Close()
	for i:=0;i<nReduce;i++ {
		fileName := reduceName(jobName, mapTaskNumber, i)
		debug("DEBUG[doMap]: Map intermediate filename: %s\n", fileName)
		mid_file, err := os.Create(fileName)
		if err != nil {
			log.Fatal("ERROR[doMap]: Create intermediate file fail ", err)
		}
		enc := json.NewEncoder(mid_file)
		for r:=0;r<rSize;r++ {
			kv := middle_res[r]
			if ihash(kv.Key) % uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("ERROR[doMap]: Encode error: ", err)
				}
			}
		}
		mid_file.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
