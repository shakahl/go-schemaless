CREATE DATABASE trips;

\c trips;

DROP TABLE IF EXISTS trips;

CREATE SEQUENCE trips_added_at_seq;

CREATE TABLE trips
(
	added_at          INTEGER DEFAULT NEXTVAL ('trips_added_at_seq'),
	row_key		  VARCHAR(36) NOT NULL,
	column_name	  VARCHAR(64) NOT NULL,
	ref_key		  INTEGER NOT NULL,
	body		  JSON,
	created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX TRIPS_IDX ON TRIPS ( row_key, column_name, ref_key ASC );

DROP TABLE IF EXISTS trips_base_driver_partner_uuid;

CREATE SEQUENCE trips_base_driver_partner_uuid_added_at_seq;

CREATE TABLE trips_base_driver_partner_uuid
(
	added_at          INTEGER DEFAULT NEXTVAL ('trips_added_at_seq'),
	row_key		  VARCHAR(36) NOT NULL,
	column_name	  VARCHAR(64) NOT NULL,
	ref_key		  INTEGER NOT NULL,
	body		  JSON,
	created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX TRIPS_BASE_DRIVER_PARTNER_UUID_IDX ON TRIPS_BASE_DRIVER_PARTNER_UUID ( row_key, column_name, ref_key ASC );

