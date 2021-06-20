package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/client"
	"github.com/rbastic/go-schemaless/models"
)

// see storagetest/storagetest.go - that code is mostly a copy of this.

const (
	sqlDateFormat = "2006-01-02 15:04:05" // TODO: Hmm, should we make this a constant somewhere?
	storeName     = "trips"
	tblName       = "trips"
	baseCol       = "BASE"
	otherCellID   = "hello"
	testString    = "{\"value\": \"The shaved yak drank from the bitter well\"}"
	testString2   = "{\"value\": \"The printer is on fire\"}"
	testString3   = "{\"value\": \"The appropriate printer-fire-response-team has been notified\"}"
)

func runPuts(cl *client.Client) string {
	cellID := uuid.New().String()
	//cellID := "981a6b8c-629b-4d30-98c2-bc4816e7157a"
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

	//fmt.Printf("startTime: %d\n", startTime)

	ctx := context.TODO()

	// otherCellID is a cell ID that intentionally doesn't exist
	v, ok, err := cl.Get(ctx, storeName, tblName, otherCellID, baseCol, 1)
	if err != nil { // If Get() returns an error then that's wrong.
		panic(err)
	}
	if ok { // If we somehow find data for this key, that's also wrong.
		panic(fmt.Sprintf("getting a non-existent key was 'ok': v=%v ok=%v\n", v, ok))
	}

	// Insert some data
	cellID := runPuts(cl)

	// Get the record with a rowKey of cellID that has the largest refKey
	v, ok, err = cl.GetLatest(ctx, storeName, tblName, cellID, baseCol)
	if err != nil {
		panic(err)
	}
	// If we find data that doesn't match testString3 for our largest refKey,
	// that's a bug
	if !ok || string(v.Body) != testString3 {
		panic(fmt.Sprintf("GetLatest failed getting a valid key: v='%s' ok=%v\n", string(v.Body), ok))
	}

	// Try to get the first record inserted
	v, ok, err = cl.Get(ctx, storeName, tblName, cellID, baseCol, 1)
	if err != nil {
		panic(err)
	}
	// Any data not matching testString is a bug
	if !ok || string(v.Body) != testString {
		panic(fmt.Sprintf("Get failed when retrieving an old value: body:%s ok=%v\n", string(v.Body), ok))
	}

	// For our next trick, we need to find the partition # that cellID was stored on.
	// Remember, records are sharded by rowKey, so all records with a UUID of cellID
	// are going to be on the same partition.
	findPartResponse, err := cl.FindPartition(storeName, tblName, cellID)
	// Check server-related errors
	if err != nil {
		panic(err)
	}
	// Check other potential database-related errors
	if findPartResponse.Error != "" {
		panic(findPartResponse.Error)
	}

	partNo := findPartResponse.PartitionNumber

	var cells []models.Cell
//	fmt.Printf("partNo:%d\n", partNo)

	// get data from the relevant partition that was written after we checked our startTime
	// the query is written internally as timestamp > startTime, which is part of why we have
	// a small 1-second sleep after our Put operations.
	cells, ok, err = cl.PartitionRead(ctx, storeName, tblName, partNo, "timestamp", startTime, 5)
	if err != nil {
		panic(err)
	}
	if !ok {
		panic(fmt.Sprintf("expected a slice of cells, response was: %+v", cells))
	}

	if len(cells) == 0 {
		panic("we have an obvious problem")
	}


	marshaledCells, err := json.Marshal(cells)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", marshaledCells)
}
