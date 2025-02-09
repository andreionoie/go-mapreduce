package main

import (
	"encoding/json"
	"go-mapreduce/mapreduce"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type DocumentData struct {
	Filename  string `json:"file"`
	Positions []int  `json:"offsets"`
}

func Map(filename string, contents string) []mapreduce.KVPair {
	filename = filepath.Base(filename)
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	wordPositions := make(map[string][]int)

	for position, word := range words {
		word = strings.ToLower(word)
		word = strings.TrimSpace(word)

		if word == "" { // skip empty words
			continue
		}

		wordPositions[word] = append(wordPositions[word], position)
	}

	pairs := []mapreduce.KVPair{}
	for word, positions := range wordPositions {
		docDataJSON, err := json.Marshal(DocumentData{Filename: filename, Positions: positions})
		if err != nil {
			panic(err)
		}
		pairs = append(pairs, mapreduce.KVPair{Key: word, Value: string(docDataJSON)})
	}

	return pairs
}

// key will be a unique word and value is a JSON of filename and byte offsets
func Reduce(key string, values []string) string {
	docList := []DocumentData{}
	for _, jsonValue := range values {
		var docData DocumentData
		err := json.Unmarshal([]byte(jsonValue), &docData)
		if err != nil {
			panic(err)
		}
		docList = append(docList, docData)
	}

	// sort for idempotency
	sort.Slice(docList, func(i, j int) bool {
		return docList[i].Filename < docList[j].Filename
	})

	docsWithOffsetsJSON, err := json.Marshal(docList)
	if err != nil {
		panic(err)
	}
	return string(docsWithOffsetsJSON)
}
