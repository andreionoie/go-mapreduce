package main

import (
	"go-mapreduce/mapreduce"
	"strconv"
	"strings"
	"time"
)

func Map(filename string, contents string) []mapreduce.KVPair {
	return []mapreduce.KVPair{{Key: filename, Value: "1"}}
}

func Reduce(key string, values []string) string {
	// some reduce tasks sleep for a long time; potentially seeing if
	// a worker will accidentally exit early
	if strings.Contains(key, "1.txt") {
		time.Sleep(time.Duration(5 * time.Second))
	}
	// return the number of occurrences of this file.
	return strconv.Itoa(len(values))
}
