package main

import (
	"flag"
	"fmt"
	"io/ioutil"
)

func main() {
	dbPtr := flag.String("db", "mysql", "the database you will be using (current options: mysql, postgres)")
	namePtr := flag.String("name", "test", "the name of your schema (will end up as a prefix for all shards)")
	numPtr := flag.Int("num", 4, "the number of shards (will be appended to shard names) - default is 4")
	createFilePtr := flag.String("createFile", "", "cell schema CREATE TABLE file")

	flag.Parse()

	if *createFilePtr == "" {
		panic("You must set a createFile. Find a suitable schema laying around somewhere.")
	}

	var showWarnings bool
	switch *dbPtr {
	case "mysql":
		showWarnings = true
		createMysql(*namePtr, *numPtr, *createFilePtr, showWarnings)
	case "postgres":
		showWarnings = false
		createPostgres(*namePtr, *numPtr, *createFilePtr, showWarnings)
	default:
		panic("Unrecognized database: " + *dbPtr)
	}

}

func createMysql(name string, num int, createFile string, showWarnings bool) {
	for i := 0; i < num; i++ {
		dbName := fmt.Sprintf("%s%d", name, i)

		// TODO(rbastic): These constants could be pulled from a templates
		// folder full of JSON files, or something else. Uber liked YAML, but
		// I'm sorry, YAML fans, there will be none of that in this open-source
		// reimplementation.
		dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s;", dbName)
		fmt.Printf("%s\n", dropDB)

		createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
		fmt.Printf("%s\n", createDB)

		if showWarnings {
			fmt.Printf("SHOW WARNINGS;\n")
		}

		useDB := fmt.Sprintf("USE %s;", dbName)
		fmt.Printf("%s\n", useDB)

		if showWarnings {
			fmt.Printf("SHOW WARNINGS;\n")
		}

		contents, err := readFile(createFile)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s\n", contents)

		if showWarnings {
			fmt.Printf("SHOW WARNINGS;\n")
		}
	}
}

func createPostgres(name string, num int, createFile string, showWarnings bool) {
	for i := 0; i < num; i++ {
		dbName := fmt.Sprintf("%s%d", name, i)

		// TODO(rbastic): These constants could be pulled from a templates
		// folder full of JSON files, or something else. Uber liked YAML, but
		// I'm sorry, YAML fans, there will be none of that in this open-source
		// reimplementation.
		dropDB := fmt.Sprintf("DROP DATABASE IF EXISTS %s;", dbName)
		fmt.Printf("%s\n", dropDB)

		createDB := fmt.Sprintf("CREATE DATABASE %s;", dbName)
		fmt.Printf("%s\n", createDB)

		useDB := fmt.Sprintf("\\c %s", dbName)
		fmt.Printf("%s\n", useDB)

		contents, err := readFile(createFile)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s\n", contents)
	}
}

func readFile(fileName string) (string, error) {
	fb, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return string(fb), nil
}
