package main

// a MapReduce pseudo-application to test that workers
// execute map tasks in parallel.

import (
	"fmt"
	"go-mapreduce/mapreduce"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
)

func Map(filename string, contents string) []mapreduce.KVPair {
	ts := float64(time.Now().UnixNano()) / 1e9
	n := nparallel("map")

	return []mapreduce.KVPair{
		{Key: fmt.Sprintf("times-%v", os.Getpid()), Value: fmt.Sprintf("%.1f", ts)},
		{Key: fmt.Sprintf("parallel-%v", os.Getpid()), Value: fmt.Sprintf("%d", n)},
	}
}

func Reduce(key string, values []string) string {
	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	myfilename := fmt.Sprintf("worker-%s-%d", phase, os.Getpid())
	if _, err := os.Create(myfilename); err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(0)
	if err != nil {
		panic(err)
	}
	workerCount := 0
	for _, name := range names {
		var workerPid int
		pat := fmt.Sprintf("worker-%s-%%d", phase)
		if n, err := fmt.Sscanf(name, pat, &workerPid); n == 1 && err == nil {
			err := syscall.Kill(workerPid, 0) // kill -0 to only check for process existence
			if err == nil {
				// if err == nil, worker was alive
				workerCount += 1
			}
		}
	}
	dd.Close()

	time.Sleep(time.Second)

	if err = os.Remove(myfilename); err != nil {
		panic(err)
	}

	return workerCount
}
