package main

// This is a MapReduce pseudo-application designed to count the number of times
// map/reduce tasks are executed. It helps verify if tasks are assigned multiple times.

import (
	"fmt"
	"go-mapreduce/mapreduce"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var mapTaskCount int

func Map(filename string, contents string) []mapreduce.KVPair {
	mapTaskCount++
	if _, err := os.Create(fmt.Sprintf("worker-jobcount-%d-%d", os.Getpid(), mapTaskCount)); err != nil {
		panic(err)
	}

	// Simulate task processing time by sleeping for a random duration between 2 to 5 seconds.
	sleepDuration := time.Duration(2000+rand.Intn(3000)) * time.Millisecond
	time.Sleep(sleepDuration)

	return []mapreduce.KVPair{{Key: "foo", Value: "bar"}}
}

func Reduce(key string, values []string) string {
	files, err := os.ReadDir(".")
	if err != nil {
		panic(err)
	}

	invocations := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "worker-jobcount-") {
			invocations++
		}
	}

	// Return the total count of map task execution files
	return strconv.Itoa(invocations)
}
