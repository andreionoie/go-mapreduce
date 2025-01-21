package main

import (
	"go-mapreduce/mapreduce"
	"log"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mapreduce.KVPair {
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
