package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type MapStatue struct {
	Finished bool
	Mutex    sync.Mutex
}

func (m *MapStatue) GetFinished() bool {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Finished
}

func (m *MapStatue) SetFinished(val bool) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Finished = val
}

type AllocatedType struct {
	Chan chan struct{}
	name string
}

type TaskStatue struct {
	Mutex     sync.Mutex
	New       map[int]string
	Allocated map[int]AllocatedType
	Finished  map[int]string
}

type Coordinator struct {
	// Your definitions here.
	finished   bool       // true if the job has been finished.
	mapStatue  MapStatue  // true if all Map tasks were finished.
	taskStatue TaskStatue // Tracking task status
	nReduce    int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Pseudo_task(key int, ch chan struct{}, taskname string) {
	tick := time.Tick(1 * time.Second)
	for countdown := 10; countdown > 0; countdown-- {
		select {
		case <-tick:
			continue
		case <-ch:
			return
		}
	}
	fmt.Printf("pseudo_task %s timeout:\n", taskname)
	c.taskStatue.Mutex.Lock()
	defer c.taskStatue.Mutex.Unlock()
	file, ok := c.taskStatue.Allocated[key]
	if ok {
		c.taskStatue.New[key] = file.name
		delete(c.taskStatue.Allocated, key)
	}
}

func (c *Coordinator) WorkerHandler(args *WorkerArgs, reply *CoordinatorReply) error {
	if c.finished {
		reply.Task = -3
		return nil
	}
	reply.Task = -1 //default value,the worker will wait for a while
	switch args.Task {
	case 0:
		if c.mapStatue.GetFinished() { // allocate reduce task
			func() {
				c.taskStatue.Mutex.Lock()
				defer c.taskStatue.Mutex.Unlock()
				if len(c.taskStatue.New) == 0 {
					reply.Task = -1
				} else {
					for key, value := range c.taskStatue.New {
						reply.Task = 1
						reply.X = key
						reply.File = value
						reply.NReduce = c.nReduce
						c.taskStatue.Allocated[key] = AllocatedType{make(chan struct{}), value}
						go c.Pseudo_task(key, c.taskStatue.Allocated[key].Chan, "reduce")
						delete(c.taskStatue.New, key)
						break
					}
					//fmt.Println(c.taskStatue.New)
				}
			}()

		} else { //allocate map task
			func() {
				c.taskStatue.Mutex.Lock()
				defer c.taskStatue.Mutex.Unlock()
				if len(c.taskStatue.New) == 0 {
					reply.Task = -1
				} else {
					// just for get a random k,v in map
					for key, value := range c.taskStatue.New {
						reply.Task = 0
						reply.X = key
						reply.File = value
						reply.NReduce = c.nReduce
						c.taskStatue.Allocated[key] = AllocatedType{make(chan struct{}), value}
						go c.Pseudo_task(key, c.taskStatue.Allocated[key].Chan, "map")
						delete(c.taskStatue.New, key)
						break
					}
				}
			}()
		}
	case 1: //1 report map task finished
		func() {
			c.taskStatue.Mutex.Lock()
			defer c.taskStatue.Mutex.Unlock()
			file, ok := c.taskStatue.Allocated[args.X]
			if ok {
				// the chan here must be available, if the chan useless, the value in map should already be deleted
				file.Chan <- struct{}{} // what will happen when the channel is useless?
				c.taskStatue.Finished[args.X] = file.name
				delete(c.taskStatue.Allocated, args.X)
			}
			// all map task finished, build reduce task map
			if len(c.taskStatue.New) == 0 && len(c.taskStatue.Allocated) == 0 {
				for i := 0; i < c.nReduce; i++ {
					c.taskStatue.New[i] = strconv.Itoa(i)
				}
				c.taskStatue.Allocated = make(map[int]AllocatedType)
				c.taskStatue.Finished = make(map[int]string)
				c.mapStatue.SetFinished(true)

			}
			reply.Task = -2
		}()
	case 2: //2 report reduce task finished
		func() {
			c.taskStatue.Mutex.Lock()
			defer c.taskStatue.Mutex.Unlock()
			file, ok := c.taskStatue.Allocated[args.X]
			if ok {
				file.Chan <- struct{}{}
				c.taskStatue.Finished[args.X] = file.name
				delete(c.taskStatue.Allocated, args.X)
			}
			if len(c.taskStatue.Finished) == c.nReduce {
				reply.Task = -3
				c.finished = true
				//fmt.Println(c.taskStatue.New)
			} else {
				reply.Task = -2
			}
		}()
	}
	//fmt.Printf("WorkerHandler was called, args: %v, reply: %v\n", args, reply)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.finished

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{taskStatue: TaskStatue{
		Mutex:     sync.Mutex{},
		New:       make(map[int]string),
		Allocated: make(map[int]AllocatedType),
		Finished:  make(map[int]string),
	}}
	// Your code here.

	// add all files into new task queue
	c.taskStatue.Mutex.Lock()
	for index, fileName := range files {
		c.taskStatue.New[index] = fileName
	}

	c.taskStatue.Mutex.Unlock()

	c.nReduce = nReduce

	c.server()
	return &c
}
