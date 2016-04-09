package mapreduce

import (
	"os"
	"log"
	"encoding/json"
	"sort"
)
// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	debug("DEBUG[doReduce]: jobName:%s, reduceTaskNumber:%d, mapTask:%d\n", jobName, reduceTaskNumber, nMap);

	//a map string -> string array
	keyValues := make(map[string][]string)

	for i:=0;i<nMap;i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		debug("DEBUG[doReduce]: Reduce File %s\n", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Open error: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break	
			}
			_, ok := keyValues[kv.Key]
			//first time insert key to map
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		file.Close()
	}

	var keys []string
	for k, _:= range keyValues {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	debug("DEBUG[doReduce]: mergeFileName: %v\n", mergeFileName)
	file, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal("ERROR[doReduce]: Create file error: ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		enc.Encode(&KeyValue{k, res})
	}
	file.Close()
}
