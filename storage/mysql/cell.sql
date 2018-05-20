DROP TABLE IF EXISTS cell;

SHOW WARNINGS;

CREATE TABLE cell
(
	added_at      INTEGER PRIMARY KEY AUTO_INCREMENT,
	row_key		  VARCHAR(36) NOT NULL,
	column_name	  VARCHAR(64) NOT NULL,
	ref_key		  INTEGER NOT NULL,
	body		  JSON,
	created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
	UNIQUE `cell_idx`(`row_key`, `column_name`, `ref_key`)
) ENGINE=InnoDB;

SHOW WARNINGS;
