package storagetest

import (
	"context"
	"github.com/gofrs/uuid"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/models"
	"testing"
	"time"
)

const (
	sqlDateFormat = "2006-01-02 15:04:05" // TODO: Hmm, should we make this a constant somewhere? Likely.
	tblName = "cell"
	baseCol     = "BASE"
	otherCellID = "hello"
	testString  = "{\"value\": \"The shaved yak drank from the bitter well\"}"
	testString2 = "{\"value\": \"The printer is on fire\"}"
	testString3 = "{\"value\": \"The appropriate printer-fire-response-team has been notified\"}"
)

func runPuts(t *testing.T, storage schemaless.Storage) string {
	cellID := uuid.Must(uuid.NewV4()).String()
	err := storage.Put(context.TODO(), tblName, cellID, baseCol, 1, testString)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Put(context.TODO(), tblName, cellID, baseCol, 2, testString2)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Put(context.TODO(), tblName, cellID, baseCol, 3, testString3)
	if err != nil {
		t.Fatal(err)
	}

	return cellID
}

// StorageTest is a simple sanity check for a schemaless Storage backend
func StorageTest(t *testing.T, storage schemaless.Storage) {
	startTime := time.Now().UTC().UnixNano()

	time.Sleep(time.Second * 1)

	ctx := context.TODO()

	defer storage.Destroy(ctx)
	v, ok, err := storage.Get(ctx, tblName, otherCellID, baseCol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("getting a non-existent key was 'ok': v=%v ok=%v\n", v, ok)
	}

	cellID := runPuts(t, storage)

	v, ok, err = storage.GetLatest(ctx, tblName, cellID, baseCol)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(v.Body) != testString3 {
		t.Errorf("failed getting a valid key: v='%s' ok=%v\n", string(v.Body), ok)
	}

	v, ok, err = storage.Get(ctx, tblName, cellID, baseCol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(v.Body) != testString {
		t.Errorf("Get failed when retrieving an old value: body:%s ok=%v\n", string(v.Body), ok)
	}

	var cells []models.Cell
	cells, ok, err = storage.PartitionRead(ctx, tblName, 0, "timestamp", startTime, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected a slice of cells, response was:", cells)
	}

	if len(cells) == 0 {
		t.Fatal("we have an obvious problem")
	}

	err = storage.ResetConnection(ctx, otherCellID)
	if err != nil {
		t.Errorf("failed resetting connection for key: err=%v\n", err)
	}
}
