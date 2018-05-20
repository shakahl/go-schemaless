DROP TABLE IF EXISTS cell;

CREATE SEQUENCE cell_added_at_seq;

CREATE TABLE cell
(
	added_at          INTEGER DEFAULT NEXTVAL ('cell_added_at_seq'),
	row_key		  VARCHAR(36) NOT NULL,
	column_name	  VARCHAR(64) NOT NULL,
	ref_key		  INTEGER NOT NULL,
	body		  JSON,
	created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX CELL_IDX ON CELL ( row_key, column_name, ref_key ASC );
