DROP TABLE IF EXISTS cell;

CREATE TABLE cell
(
	added_at      	INTEGER PRIMARY KEY AUTO_INCREMENT,
	driver_partner_uuid VARCHAR(36) NULL,
	city_uuid	VARCHAR(36) NOT NULL,
	trip_created_at DATETIME NULL
) ENGINE=InnoDB;


