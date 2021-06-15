package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/sqlite"

	"strconv"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

const (
	tblName = "trips"
	Status  = "STATUS"
)

var (
	ErrCouldNotBillRider = errors.New("could not bill rider")
)

func getShards(prefix string) []core.Shard {
	var shards []core.Shard
	nShards := 4

	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)
		stor, err := st.New(tblName, label)
		if err != nil {
			panic(err)
		}
		shards = append(shards, core.Shard{Name: label, Backend: stor})
	}

	return shards
}

func main() {
	shards := getShards("trips")
	sl := schemaless.New().WithSources("trips", shards)

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	defer sl.Destroy(context.TODO())

	callToCreditCardProcessorForBillingTrip := func(tripInfo models.Cell) string {
		// Do something with the tripInfo
		return "SUCCESS"
	}

	billRideFunc := func(rowKey string) error {
		status, ok, err := sl.GetLatest(context.TODO(), tblName, rowKey, Status)
		// NOTE: We deviate from the original example here immediately, because exceptions do not have to be part of the equation.
		if err != nil {
			return err
		}
		if !ok {
			return ErrCouldNotBillRider
		}

		is_completed := gjson.Get(string(status.Body), "is_completed").String()
		if is_completed == "true" {
			// This should mean we've already billed the order?
			return nil
		}

		// Otherwise we try to bill.

		// We try to fetch the base trip information from the BASE column.
		tripInfo, ok, err := sl.GetLatest(context.TODO(), tblName, rowKey, "BASE")

		logger.Info("after GetLatest BASE", zap.Any("tripInfo", tripInfo))

		// We bill the rider
		result := callToCreditCardProcessorForBillingTrip(tripInfo)

		if result != "SUCCESS" {
			// By raising an exception we let Schemaless triggers retry later
			return ErrCouldNotBillRider
		}

		// We billed the rider successfully and write it back to Mezzanine
		body := "{\"is_completed\":true}"
		body, err = sjson.Set(body, "result", result)

		if err != nil {
			return err
		}

		status.Body = body

		// TODO(rbastic): It doesn't appear that this is in the code
		// example, is there something I'm missing here...?
		status.RefKey++
		return sl.Put(context.TODO(), tblName, rowKey, Status, status.RefKey, status.Body)
	}

	rowKey := uuid.New().String()
	testStatus := models.NewCell(rowKey, Status, 1, "{\"Test\": \"Value\"}")
	err = sl.Put(context.TODO(), tblName, rowKey, Status, testStatus.RefKey, testStatus.Body)
	if err != nil {
		fmt.Println("Had an error:", err)
		return
	}

	err = billRideFunc(rowKey)
	if err != nil {
		fmt.Println("Had an error:", err)
		return
	}

	fmt.Println("Done.")
}
