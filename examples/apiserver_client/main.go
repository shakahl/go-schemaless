package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/client"
	"github.com/rbastic/go-schemaless/models"

	"github.com/tidwall/sjson"
)

var (
	sqlDateFormat = "2006-01-02 15:04:05"
	storeName     = "trips"
	tblName       = "trips"
	baseCol       = "BASE"
	otherCellID   = "hello"
	testStrings   = []string{
		"{\"value\": \"The shaved yak drank from the bitter well\"}",
		"{\"value\": \"The printer is on fire\"}",
		"{\"value\": \"The appropriate printer-fire-response-team has been notified\"}",
	}
)

func runPuts(cl *client.Client) (string, string) {
	cellID := uuid.New().String()

	var err error

	driverPartnerUUID := uuid.New().String()

	for idx := range testStrings {
		testStrings[idx], err = sjson.Set(string(testStrings[idx]), "driver_partner_uuid", driverPartnerUUID)
		if err != nil {
			log.Fatal(err)
		}

		testStrings[idx], err = sjson.Set(string(testStrings[idx]), "city_uuid", uuid.New().String())
		if err != nil {
			log.Fatal(err)
		}


		testStrings[idx], err = sjson.Set(string(testStrings[idx]), "trip_created_at", time.Now().UTC())
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 1, testStrings[0])
	if err != nil {
		log.Fatal(err)
	}

	_, err = cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 2, testStrings[1])
	if err != nil {
		log.Fatal(err)
	}

	_, err = cl.Put(context.TODO(), storeName, tblName, cellID, baseCol, 3, testStrings[2])
	if err != nil {
		log.Fatal(err)
	}

	return cellID, driverPartnerUUID
}

type Specification struct {
	StartTime int64
}

func main() {
	var s Specification
	err := envconfig.Process("app", &s)
	if err != nil {
		log.Fatal(err)
	}

	cl := client.New().WithAddress("http://localhost:4444")

	startTime := time.Now().UTC().UnixNano()
	if s.StartTime != 0 {
		startTime = s.StartTime
	}

	//fmt.Printf("startTime: %d\n", startTime)

	ctx := context.TODO()

	v, ok, err := cl.Get(ctx, storeName, tblName, otherCellID, baseCol, 1)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		log.Fatal(fmt.Sprintf("getting a non-existent key was 'ok': v=%v ok=%v\n", v, ok))
	}

	cellID, driverPartnerUUID := runPuts(cl)

	time.Sleep(1 * time.Second)

	v, ok, err = cl.GetLatest(ctx, storeName, tblName, cellID, baseCol)
	if err != nil {
		log.Fatal(err)
	}
	if !ok || string(v.Body) != testStrings[2] {
		log.Fatal(fmt.Sprintf("GetLatest failed getting a valid key: v='%s' ok=%v\n", string(v.Body), ok))
	}

	v, ok, err = cl.Get(ctx, storeName, tblName, cellID, baseCol, 1)
	if err != nil {
		log.Fatal(err)
	}
	if !ok || string(v.Body) != testStrings[0] {
		log.Fatal(fmt.Sprintf("Get failed when retrieving an old value: body:%s ok=%v\n", string(v.Body), ok))
	}

	{
		findPartResponse, err := cl.FindPartition(storeName, tblName, cellID)
		if err != nil {
			log.Fatal(err)
		}
		if findPartResponse.Error != "" {
			log.Fatal(findPartResponse.Error)
		}

		partNo := findPartResponse.PartitionNumber

		var cells []models.Cell
		cells, ok, err = cl.PartitionRead(ctx, storeName, tblName, partNo, "timestamp", startTime, 3)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal(fmt.Sprintf("expected a slice of cells, response was: %+v", cells))
		}

		if len(cells) == 0 {
			log.Fatal("we have an obvious problem")
		}

		resp, err := json.Marshal(cells)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", resp)
	}

	// Check index partition
	{
		indexTableName := "trips_base_driver_partner_uuid"
		findPartResponse, err := cl.FindPartition(storeName, indexTableName, driverPartnerUUID)
		if err != nil {
			log.Fatal(err)
		}
		if findPartResponse.Error != "" {
			log.Fatal(findPartResponse.Error)
		}

		partNo := findPartResponse.PartitionNumber
		//fmt.Printf("QUERYING partNo:%d startTime:%d\n", partNo, startTime)

		var cells []models.Cell
		cells, ok, err = cl.PartitionRead(ctx, storeName, indexTableName, partNo, "timestamp", startTime, 3)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal(fmt.Sprintf("expected a slice of cells, response was: %+v", cells))
		}

		if len(cells) == 0 {
			log.Fatal("we have an obvious problem")
		}

		resp, err := json.Marshal(cells)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", resp)
	}

}
