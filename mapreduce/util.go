package mapreduce

import (
	"log"
	"plugin"
)

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

	mapf, ok := mapSymbol.(func(string, string) []KVPair)
	if !ok {
		log.Fatalf("Map symbol from plugin failed the type assertion")
	}
	reducef, ok := reduceSymbol.(func(string, []string) string)
	if !ok {
		log.Fatalf("Reduce symbol from plugin failed the type assertion")
	}

	return mapf, reducef
}
