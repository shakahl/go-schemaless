DROP TABLE IF EXISTS cell;

CREATE TABLE cell ( added_at INTEGER PRIMARY KEY AUTOINCREMENT, row_key VARCHAR(36) NOT NULL, column_name VARCHAR(64) NOT NULL, ref_key INTEGER NOT NULL, body JSON, created_at DATETIME DEFAULT (datetime('now','localtime'))); 
CREATE UNIQUE INDEX IF NOT EXISTS uniqcell_idx ON cell ( row_key, column_name, ref_key );


