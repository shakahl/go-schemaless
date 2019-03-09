package models

// "[Cell is ... ] the smallest data entity in Schemaless - it is
// immutable; once written, it cannot be overwritten or deleted. The
// cell is a JSON blob referenced by a row key, a column name, and a
// reference key called ref key.  The row key is a UUID, while the
// column name is a string and the reference key is an integer.
//
// You can think of the row key as a primary key in a relational
// database, and the column name as a column. However, in Schemaless
// there is no predefined or enforced schema and rows do not need to
// share column names; in fact, the column names are completely defined
// by the application. The ref key is used to version the cells for a
// given row key and column. So if a cell needs to be updated, you would
// write a new cell with a higher ref key (the latest cell is the one
// with the highest ref key). The ref key is also useable as entries in
// a list, but is typically used for versioning. The application decides
// which scheme to employ here." -- [1] 'The Schemaless Data Model',
// https://eng.uber.com/schemaless-part-one/
//
type Cell struct {
	Type       string `json:"T,omitempty"` // TODO(rbastic): unused
	AddedAt    uint64 `json:"A,omitempty"`
	RowKey     string `json:"R,omitempty"` // UUID, typically, but could be anything
	ColumnName string `json:"C,omitempty"` // The actual column name for the individual Body blob
	RefKey     int64  `json:"K,omitempty"` // for versioning or sorting cells in a list
	Body       string `json:"B,omitempty"` // Uber chose JSON inside MessagePack'd LZ4 blobs, we store raw JSON
	CreatedAt  uint64 `json:"TS,omitempty"`
}

// NewCell constructs a Cell structure with the minimum parameters necessary:
// a row key and column name (strings), a ref key (int64), and a body
// ([]byte).
func NewCell(rowKey string, columnName string, refKey int64, body string) Cell {
	return Cell{RowKey: rowKey, ColumnName: columnName, RefKey: refKey, Body: body}
}

// 'Applications typically group related data into the same column, and then
// all cells in each column have roughly the same application-side schema.
// This grouping is a great way to bundle data that changes together, and it
// allows the application to rapidly change the schema without downtime on
// the database side.  The example below elaborates more on this.' [1]
