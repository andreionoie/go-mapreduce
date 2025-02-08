package mapreduce

import (
	"hash/fnv"
	"log"
	"plugin"
)

func Fnv1aHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func LoadPlugin(filename string) (func(string, string) []KVPair, func(string, []string) string) {
	plgn, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("Error opening plugin: %v", err)
	}

	mapSymbol, err := plgn.Lookup("Map")
	if err != nil {
		log.Fatalf("Error in plugin lookup for Map: %v", err)
	}
	reduceSymbol, err := plgn.Lookup("Reduce")
	if err != nil {
		log.Fatalf("Error in plugin lookup for Reduce: %v", err)
	}

	mapFunc, ok := mapSymbol.(func(string, string) []KVPair)
	if !ok {
		log.Fatalf("Map symbol from plugin failed the type assertion")
	}
	reduceFunc, ok := reduceSymbol.(func(string, []string) string)
	if !ok {
		log.Fatalf("Reduce symbol from plugin failed the type assertion")
	}

	return mapFunc, reduceFunc
}
