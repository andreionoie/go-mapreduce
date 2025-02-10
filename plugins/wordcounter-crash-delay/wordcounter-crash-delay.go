package main

import (
	"go-mapreduce/mapreduce"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func Map(filename string, contents string) []mapreduce.KVPair {
	maybeCrash(0.3, 0.3, 5000)
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	pairs := []mapreduce.KVPair{}
	for _, w := range words {
		pairs = append(pairs, mapreduce.KVPair{Key: w, Value: "1"})
	}

	return pairs
}

func Reduce(key string, values []string) string {
	maybeCrash(0.0001, 0.001, 200) // since the chance accumulates over many reduce calls, need small value
	// return the number of occurrences of this word.
	value := 0
	for _, av := range values {
		iv, err := strconv.Atoi(av)
		if err != nil {
			log.Fatalf("Parsing error for %s: %v", av, err)
		}
		value += iv
	}

	return strconv.Itoa(value)
}

func maybeCrash(crashChance float64, delayChance float64, maxDelayMillis int) {
	rnd := rand.Float64()
	if rnd < crashChance {
		log.Printf("Worker crashed with chance %.2f%%!", crashChance*100)
		os.Exit(0)
	} else if rnd < crashChance+delayChance {
		time.Sleep(time.Duration(rand.Intn(maxDelayMillis)) * time.Millisecond)
		log.Printf("Worker delayed with chance %.2f%%!", delayChance*100)
	}
}
