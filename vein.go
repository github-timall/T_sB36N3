package main

import (
	"database/sql"
)

const(
	VEIN_STATUS_READY int = 1
	VEIN_STATUS_PENDING int = 2

	VEIN_SECOND_ATTEMPT int = 1
	VEIN_THIRD_ATTEMPT int = 2
)

type (
	Event interface {
		getEventType() 	string
		getEventId() 	int

		getEntityType() string
		getEntityId() 	int

		getJsonString() string
	}

	Response struct {
		VeinEvent 		VeinEvent
		StatusCode 		int
		Body 			string
	}

 	VeinEvent struct {
		Id 			int
		ServiceName string
		EventType 	string
		EventId 	int
		EntityType	string
		EntityId	int
		TrySuccess	int
		TryNumber	int
		Data 		string
	}
)

func InitVeinEvent(db *sql.DB) (err error) {
	query := "CREATE TABLE IF NOT EXISTS `vein_events` (" +
		"`id` 			INT NOT NULL AUTO_INCREMENT," +
		"`status` 		SMALLINT NOT NULL DEFAULT 1," +
		"`service_name` VARCHAR(255) DEFAULT NULL," +

		"`event_type` 	VARCHAR(255) DEFAULT NULL," +
		"`event_id` 	INT NOT NULL DEFAULT 0," +

		"`entity_type` 	VARCHAR(255) DEFAULT NULL," +
		"`entity_id` 	INT NOT NULL DEFAULT 0," +

		"`try_success` 	TINYINT(1) DEFAULT 0," +
		"`try_number` 	SMALLINT NOT NULL DEFAULT 0," +
		"`try_time` 	DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"`try_response` TEXT DEFAULT NULL," +

		"`data` 		TEXT DEFAULT NULL," +
		"`created_at` 	DATETIME DEFAULT CURRENT_TIMESTAMP," +

		"PRIMARY KEY (`id`)," +
		"UNIQUE `vein_event_entity_type` (`event_id`, `entity_type`)" +
		") ENGINE='InnoDB' COLLATE 'utf8_unicode_ci';"
	_ ,err = db.Exec(query)
	return
}