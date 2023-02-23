package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	c := connect()
	curDir, _ := os.Getwd()
	//log.Printf("current  %v\n", curDir)
	args := &WorkerArgs{}
	reply := &CoordinatorReply{}

	// Your worker implementation here.
	// 1. call the coordinator to get a task.
	args.Task = 0
	ok := call("Coordinator.WorkerHandler", args, reply, c)
	if !ok {
		return
	}

NewRequset:
	args = &WorkerArgs{}
	// 2. do the task.
	switch reply.Task {
	case -3:
		log.Printf("worker exit\n")
		return
	case -2:
		reply = &CoordinatorReply{}
		args.Task = 0
		ok := call("Coordinator.WorkerHandler", args, reply, c)
		if !ok {
			return
		}
		goto NewRequset
	case -1:
		time.Sleep(5 * time.Second)
		reply = &CoordinatorReply{}
		args.Task = 0
		ok := call("Coordinator.WorkerHandler", args, reply, c)
		if !ok {
			return
		}
		goto NewRequset
	case 0: //do map task
		func() {
			//log.Printf("file need to open:%s\n", reply.File)
			file, err := os.Open(reply.File)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			intermediate := mapf(reply.File, string(content))

			// create temp middle file
			tempFiles := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				tempFiles[i], _ = os.CreateTemp(curDir, "temp")
			}

			for _, kv := range intermediate {
				reduceNum := ihash(kv.Key) % reply.NReduce
				enc := json.NewEncoder(tempFiles[reduceNum])
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("encode error:", err)
				}
			}

			// rename temp file to middle file
			for i := 0; i < reply.NReduce; i++ {
				tempFiles[i].Close()
				os.Rename(tempFiles[i].Name(), fmt.Sprintf("mr-%v-%v", reply.X, i))
			}
			//log.Printf("worker %d encode success\n", reply.X)
			// report task finished
			args.Task = 1
			args.X = reply.X
			//reply := CoordinatorReply{}
			reply = &CoordinatorReply{}
			ok := call("Coordinator.WorkerHandler", args, reply, c)
			if !ok {
				return
			}
		}()
	case 1: //
		func() { //reduce task
			x := reply.X
			pattern := fmt.Sprintf("^mr-\\d+-%d$", x)
			re, _ := regexp.Compile(pattern)
			files, err := os.ReadDir(curDir)
			if err != nil {
				return
			}
			var kva []KeyValue
			for _, file := range files {
				if re.MatchString(file.Name()) {
					func(f os.DirEntry) {
						file, err := os.Open(f.Name())
						defer file.Close()
						if err != nil {
							log.Fatal(err)
						}
						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							kva = append(kva, kv)
						}
					}(file)
				}
			}
			// sort by key
			sort.Sort(ByKey(kva))
			ofile, _ := os.CreateTemp(curDir, "out-temp")
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			ofile.Close()
			os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", x))
			//log.Printf("worker %d reduce success\n", x)
			// report task finished
			args.Task = 2
			args.X = x
			reply = &CoordinatorReply{}
			ok := call("Coordinator.WorkerHandler", args, reply, c)
			if !ok {
				return
			}
		}()

	}
	goto NewRequset
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}, c *rpc.Client) bool {

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println(err)
	return false
}

func connect() *rpc.Client {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dial error:", err)
	}
	return c
}
