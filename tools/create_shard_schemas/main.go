package main

import (
	"flag"
	"fmt"
)

func main() {
	namePtr := flag.String("name", "test", "the name of your schema (will end up as a prefix for all shards)")
	numPtr := flag.Int("num", 4, "the number of shards (will be appended to shard names) - default is 4")
	createFilePtr := flag.String("createFile", "", "cell schema CREATE TABLE file")
	flag.Parse()

	if *createFilePtr == "" {
		panic("You must set a createFile. Find a suitable schema laying around somewhere.")
	}

	for i := 0; i < *numPtr; i++ {
		name := fmt.Sprintf("%s%d", *namePtr, i)
		// TODO(rbastic): These constants could be pulled from a templates
		// folder full of JSON files, or something else. Uber liked YAML, but
		// I'm sorry, YAML fans, there will be none of that in this open-source
		// reimplementation.
		dropDB := fmt.Sprintf("DROP DATABASE %s;", name)
		fmt.Printf("%s\n", dropDB)

		createDB := fmt.Sprintf("CREATE DATABASE %s;", name)
		fmt.Printf("%s\n", createDB)

		fmt.Printf("SHOW WARNINGS;\n")

		useDB := fmt.Sprintf("USE %s;", name)
		fmt.Printf("%s\n", useDB)

		fmt.Printf("SHOW WARNINGS;\n")

		source := fmt.Sprintf("SOURCE %s;", *createFilePtr)
		fmt.Printf("%s\n", source)

		fmt.Printf("SHOW WARNINGS;\n")
	}
}
