package main

import (
	"go-mapreduce/mapreduce"
	"log"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./run_master in1.txt in2.txt in3.txt...")
	}

	m := mapreduce.InitMaster(10, os.Args[1:])
	for !m.Done() {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
