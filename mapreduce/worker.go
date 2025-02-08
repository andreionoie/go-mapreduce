package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var workerName string

func RunWorker(mapFunc func(string, string) []KVPair, reduceFunc func(string, []string) string) {
	initWorkerName()
	log.Print("Worker started...")

	for {
		updateArgs, err := CallTaskRequest(mapFunc, reduceFunc)
		if err != nil {
			log.Printf("Task failed because %v", err)
			// updateArgs.NewState = Error
		}

		if updateArgs != nil {
			updateArgs.NewState = Completed
			if updateArgs.Type == Map || updateArgs.Type == Reduce {
				CallTaskUpdate(updateArgs)
			}
		}

	}
}

func CallTaskUpdate(args *WorkerTaskUpdateArgs) {
	reply := WorkerTaskStatusReply{}
	ok := CallMaster("Master.WorkerTaskUpdate", args, &reply)
	if ok {
		log.Printf("WorkerTaskUpdate reply: %v", reply)
		if reply.Response == AllTasksFinished {
			log.Print("WorkerTaskUpdate call indicated this was the last task, thus exiting...")
			os.Exit(0)
		}
	} else {
		log.Print("WorkerTaskUpdate call failed")
	}
}

func CallTaskRequest(mapFunc func(string, string) []KVPair, reduceFunc func(string, []string) string) (*WorkerTaskUpdateArgs, error) {
	args := WorkerTaskRequestArgs{WorkerName: workerName}
	reply := WorkerAssignedTaskReply{}
	ok := CallMaster("Master.WorkerTaskRequest", &args, &reply)
	if ok {
		var outputFilePaths []string
		if reply.Response == AssignedTask {
			if reply.Type == Map {
				outputFilePaths = doMapTask(reply, mapFunc)
			} else if reply.Type == Reduce {
				outputFilePaths = doReduceTask(reply, reduceFunc)
			} else {
				panic("unknown task type")
			}
		} else if reply.Response == WaitingForPendingMaps {
			log.Print("No more map tasks found upon request, waiting for the in progress map tasks to finish...")
			time.Sleep(time.Second)
		} else if reply.Response == WaitingForPendingReduces {
			log.Print("No more reduce tasks found upon request, waiting for the coordinator to reply with AllTasksFinished...")
			time.Sleep(time.Second)
		} else if reply.Response == AllTasksFinished {
			log.Print("All tasks finished, exiting...")
			os.Exit(0)
		} else {
			panic("unknown task response")
		}

		return &WorkerTaskUpdateArgs{
			Type:            reply.Type,
			TaskNumber:      reply.TaskNumber,
			WorkerName:      workerName,
			OutputFilePaths: outputFilePaths,
		}, nil
	} else {
		return nil, fmt.Errorf("WorkerTaskRequest failed")
	}
}

func doReduceTask(reply WorkerAssignedTaskReply, reducef func(string, []string) string) []string {
	if len(reply.InputFilePaths) <= 0 {
		panic("expected one or more input files for reduce task")
	}

	var intermediateKva []KVPair
	for _, path := range reply.InputFilePaths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		var currentFileKva []KVPair
		dec := json.NewDecoder(file)
		if err := dec.Decode(&currentFileKva); err != nil {
			panic(err)
		}
		intermediateKva = append(intermediateKva, currentFileKva...)
	}

	sort.Sort(ByKey(intermediateKva))

	width := len(fmt.Sprintf("%d", reply.NReduce-1))
	outFileName := fmt.Sprintf("mapreduce-out-%0*d-of-%d", width, reply.TaskNumber, reply.NReduce-1)
	outFile, err := os.Create(outFileName)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	i := 0
	for i < len(intermediateKva) {
		j := i + 1
		for j < len(intermediateKva) && intermediateKva[j].Key == intermediateKva[i].Key {
			j++
		}

		valuesWithSameKey := []string{}
		for k := i; k < j; k++ {
			valuesWithSameKey = append(valuesWithSameKey, intermediateKva[k].Value)
		}

		fmt.Fprintf(outFile, "%v %v\n",
			intermediateKva[i].Key,
			reducef(intermediateKva[i].Key, valuesWithSameKey),
		)

		i = j
	}

	return []string{outFileName}
}

func doMapTask(reply WorkerAssignedTaskReply, mapFunc func(string, string) []KVPair) []string {
	if len(reply.InputFilePaths) != 1 {
		panic("unexpected length of input filenames")
	}
	path := reply.InputFilePaths[0]
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// temp dir to store the intermediate files
	tempDir, err := os.MkdirTemp("", workerName)
	if err != nil {
		panic(err)
	}

	// read the input split (file) contents
	splitContent, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	// apply the Map() function
	kva := mapFunc(path, string(splitContent))

	// split into nReduce buckets
	kvaShards := make([][]KVPair, reply.NReduce)
	for _, kv := range kva {
		bucket := Fnv1aHash(kv.Key) % reply.NReduce
		kvaShards[bucket] = append(kvaShards[bucket], kv)
	}
	var intermediateFilePaths []string
	// write into nReduce files
	for reduceTaskNumber, intermediateKva := range kvaShards {
		if len(intermediateKva) == 0 {
			continue
		}

		intermediateFileName := fmt.Sprintf("intermediate-M%d-R%d.json", reply.TaskNumber, reduceTaskNumber)
		intermediateFilePath := filepath.Join(tempDir, intermediateFileName)
		intermediateFile, err := os.OpenFile(intermediateFilePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer intermediateFile.Close()
		encoder := json.NewEncoder(intermediateFile)
		encoder.SetIndent("", "\t")
		encoder.Encode(intermediateKva)
		intermediateFilePaths = append(intermediateFilePaths, intermediateFilePath)
	}

	return intermediateFilePaths
}

func initWorkerName() {
	adjectives := []string{"Warty", "Hoary", "Breezy", "Dapper", "Edgy", "Feisty", "Gutsy", "Hardy", "Intrepid", "Jaunty", "Karmic", "Lucid", "Maverick", "Natty", "Oneiric", "Precise", "Quantal", "Raring", "Saucy", "Trusty", "Utopic", "Vivid", "Wily", "Xenial", "Yakkety", "Zesty", "Artful", "Bionic", "Cosmic", "Disco", "Eoan", "Focal", "Groovy", "Hirsute", "Impish", "Jammy", "Kinetic", "Lunar", "Mantic"}
	nouns := []string{"Warthog", "Hedgehog", "Badger", "Drake", "Eft", "Fawn", "Gibbon", "Heron", "Ibex", "Jackalope", "Koala", "Lynx", "Meerkat", "Narwhal", "Ocelot", "Pangolin", "Quetzal", "Ringtail", "Salamander", "Tahr", "Unicorn", "Vervet", "Werewolf", "Xerus", "Yak", "Zapus", "Aardvark", "Beaver", "Cuttlefish", "Dingo", "Ermine", "Fossa", "Gorilla", "Hippo", "Indri", "Jellyfish", "Kudu", "Lobster", "Minotaur"}

	adj := adjectives[rand.Intn(len(adjectives))]
	noun := nouns[rand.Intn(len(nouns))]

	workerName = fmt.Sprintf("%s%s_%d", adj, noun, os.Getpid())

	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(fmt.Sprintf("[%s] ", workerName))
}
