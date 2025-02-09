package main

import (
	"encoding/json"
	"go-mapreduce/mapreduce"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mapreduce.KVPair {
	filename = filepath.Base(filename)

	// helper to test for word separators
	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }

	// accumulate all data in a map keyed by the word
	wordData := make(map[string]*DocumentData)

	wordIndex := 0
	var startOffset int
	var currentWord strings.Builder

	// append a "sentinel" space to handle the last word in the same loop
	contents += " "
	for i, r := range contents {
		// if we hit a separator, record any accumulated word
		if isNotLetter(r) {
			if currentWord.Len() > 0 {
				word := strings.ToLower(currentWord.String())
				addWordData(wordData, word, filename, wordIndex, startOffset)
				wordIndex++
				currentWord.Reset()
			}
		} else {
			// if starting a new word, note the start byte offset
			if currentWord.Len() == 0 {
				startOffset = i
			}
			currentWord.WriteRune(r)
		}
	}

	pairs := make([]mapreduce.KVPair, 0, len(wordData))
	for word, data := range wordData {
		docDataBytes, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		pairs = append(pairs, mapreduce.KVPair{Key: word, Value: string(docDataBytes)})
	}
	return pairs
}

// Reduce merges all the DocumentData objects for a word and returns them sorted by filename.
func Reduce(key string, values []string) string {
	var docList []DocumentData
	for _, jsonValue := range values {
		var docData DocumentData
		if err := json.Unmarshal([]byte(jsonValue), &docData); err != nil {
			panic(err)
		}
		docList = append(docList, docData)
	}

	// sort for idempotent output
	sort.Slice(docList, func(i, j int) bool {
		return docList[i].Filename < docList[j].Filename
	})

	docsWithOffsetsJSON, err := json.Marshal(docList)
	if err != nil {
		panic(err)
	}
	return string(docsWithOffsetsJSON)
}

type DocumentData struct {
	Filename    string `json:"file"`
	Positions   []int  `json:"positions"`
	ByteOffsets []int  `json:"byteOffsets"`
}

// addWordData appends positions and byte offsets for the given word into wordData.
func addWordData(wordData map[string]*DocumentData, word, filename string, position, startOffset int) {
	doc, exists := wordData[word]
	if !exists {
		doc = &DocumentData{Filename: filename}
		wordData[word] = doc
	}
	doc.Positions = append(doc.Positions, position)
	doc.ByteOffsets = append(doc.ByteOffsets, startOffset)
}
