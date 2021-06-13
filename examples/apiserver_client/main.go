package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/client"
	"github.com/rbastic/go-schemaless/models"
)

// see storagetest/storagetest.go - that code is mostly a copy of this.

const (
	sqlDateFormat = "2006-01-02 15:04:05" // TODO: Hmm, should we make this a constant somewhere?
	storeName     = "trips"
	tblName       = "cell"
	baseCol       = "BASE"
	otherCellID   = "hello"
	testString    = "{\"value\": \"The shaved yak drank from the bitter well\"}"
	testString2   = "{\"value\": \"The printer is on fire\"}"
	testString3   = "{\"value\": \"The appropriate printer-fire-response-team has been notified\"}"
)

func runPuts(cl *client.Client) string {
	cellID := uuid.Must(uuid.NewV4()).String()
	_, err := cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 1, testString)
	if err != nil {
		panic(err)
	}

	_, err = cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 2, testString2)
	if err != nil {
		panic(err)
	}

	_, err = cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 3, testString3)
	if err != nil {
		panic(err)
	}

	return cellID
}

type Specification struct {
	StartTime int64
}

func main() {
	var s Specification
	err := envconfig.Process("app", &s)
	if err != nil {
		panic(err)
	}

	cl := client.New().WithAddress("http://localhost:4444")

	startTime := time.Now().UTC().UnixNano()
	if s.StartTime != 0 {
		startTime = s.StartTime
	}

	fmt.Printf("startTime: %d\n", startTime)

	ctx := context.TODO()

	v, ok, err := cl.Get(ctx, storeName, tblName, otherCellID, baseCol, 1)
	if err != nil {
		panic(err)
	}
	if ok {
		panic(fmt.Sprintf("getting a non-existent key was 'ok': v=%v ok=%v\n", v, ok))
	}

	cellID := runPuts(cl)

	time.Sleep(1 * time.Second)

	v, ok, err = cl.GetLatest(ctx, storeName, tblName, cellID, baseCol)
	if err != nil {
		panic(err)
	}
	if !ok || string(v.Body) != testString3 {
		panic(fmt.Sprintf("GetLatest failed getting a valid key: v='%s' ok=%v\n", string(v.Body), ok))
	}

	v, ok, err = cl.Get(ctx, storeName, tblName, cellID, baseCol, 1)
	if err != nil {
		panic(err)
	}
	if !ok || string(v.Body) != testString {
		panic(fmt.Sprintf("Get failed when retrieving an old value: body:%s ok=%v\n", string(v.Body), ok))
	}

	var cells []models.Cell
	cells, ok, err = cl.PartitionRead(ctx, storeName, tblName, 0, "timestamp", startTime, 5)
	if err != nil {
		panic(err)
	}
	if !ok {
		panic(fmt.Sprintf("expected a slice of cells, response was: %+v", cells))
	}

	if len(cells) == 0 {
		panic("we have an obvious problem")
	}

	fmt.Printf("cells: %+v\n", cells)
}
