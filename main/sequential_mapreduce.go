package main

import (
	"fmt"
	"go-mapreduce/mapreduce"
	"io"
	"log"
	"os"
	"sort"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: ./sequential_mapreduce plugin.so in1.txt in2.txt in3.txt ...")
	}

	mapf, reducef := mapreduce.LoadPlugin(os.Args[1])

	intermediate := []mapreduce.KVPair{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open %s: %v", filename, err)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Cannot read %s: %v", filename, err)
		}
		file.Close()

		mappedPairs := mapf(filename, string(content))
		intermediate = append(intermediate, mappedPairs...)
	}

	sort.Sort(mapreduce.ByKey(intermediate))

	outfile, err := os.Create("mr-out-0")
	if err != nil {
		log.Fatalf("Cannot open outfile: %v", err)
	}
	defer outfile.Close()

	i := 0
	for i < len(intermediate) {
		values := []string{}
		j := i
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}
