package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/rbastic/go-schemaless/models"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	st "github.com/rbastic/go-schemaless/storage/memory"

	"github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
	"strconv"
)

const (
	Status = "STATUS"
)

var (
	ErrCouldNotBillRider = errors.New("could not bill rider")
)

// TODO(rbastic): refactor this into a set of Strategy patterns,
// including mock patterns for tests and examples like this one.
func getShards(prefix string) []core.Shard {
	var shards []core.Shard
	nShards := 4

	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)
		shards = append(shards, core.Shard{Name: label, Backend: st.New()})
	}

	return shards
}

func newUUID() string {
	return uuid.Must(uuid.NewV4()).String()
}

// TODO(rbastic): In the example provided by Uber, this was:
// ... = NewSchemaless("mezzanine")
// So a data store identifier will eventually map to
// a Strategy that can retrieve the list of shards responsible, and a series of
// Strategy patterns could be implemented for things like etcd, Consul, ...
// For now, we initialize our shard-storages directly.

func main() {
	shards := getShards("trips")
	sl := schemaless.New( shards )

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
		status, ok, err := sl.GetCellLatest(context.TODO(), rowKey, Status)
		// NOTE: We deviate from the original example here immediately, because
		// exceptions do not have to be part of the equation.
		if err != nil {
			return err
		}
		if !ok {
			// TODO(rbastic): return SchemalessError(err)?
			return ErrCouldNotBillRider
		}

		is_completed := gjson.Get(string(status.Body), "is_completed").String()
		if is_completed == "true" {
			// This should mean we've already billed the order?
			return nil
		}

		// Otherwise we try to bill.

		// We try to fetch the base trip information from the BASE column.
		tripInfo, ok, err := sl.GetCellLatest(context.TODO(), rowKey, "BASE")

		logger.Info("after GetCellLatest BASE", zap.Any("tripInfo", tripInfo))

		// We bill the rider
		result := callToCreditCardProcessorForBillingTrip(tripInfo)

		if result != "SUCCESS" {
			// By raising an exception we let Schemaless triggers retry later
			return ErrCouldNotBillRider
		}

		// We billed the rider successfully and write it back to Mezzanine
		body := "{\"is_completed\":true}"
		body, err = sjson.Set(body, "result", result)

		// It seems the Python example loves exceptions, I'm not sure the Go
		// code *should* be the same, but we'll stick with that idea for now.
		if err != nil {
			return err
		}

		status.Body = []byte(body)

		// TODO(rbastic): It doesn't appear that this is in the code
		// example, is there something I'm missing here...?
		status.RefKey++
		sl.PutCell(context.TODO(), rowKey, Status, status.RefKey, status)
		return nil
	}

	// TODO(rbastic): Need support for some APIs that enable
	// the functionality Uber talks about in their articles.
	// The APIs are not documented.
	//sl = sl.WithTrigger("BASE", billRideFunc)

	// In a real implementation, we would pull for notifications (or have them
	// pushed to us) and we would then invoke our trigger.
	//sl.CallTrigger("BASE", newUUID())

	rowKey := newUUID()
	testStatus := models.NewCell( rowKey, Status, 1, []byte("{\"Test\"}"))
	err = sl.PutCell(context.TODO(), rowKey, Status, testStatus.RefKey, testStatus) 
	if err != nil {
		fmt.Println("Had an error:", err)
	}

	err = billRideFunc(rowKey)
	if err != nil {
		fmt.Println("Had an error:", err)
	}

	fmt.Println("Oh well.")
}
