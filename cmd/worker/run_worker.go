package main

import (
	"go-mapreduce/mapreduce"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: ./run_worker plugin.so")
	}

	mapFunc, reduceFunc := mapreduce.LoadPlugin(os.Args[1])
	mapreduce.RunWorker(mapFunc, reduceFunc)
}
