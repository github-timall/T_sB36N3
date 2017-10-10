package main

import (
	"time"
	"database/sql"
	"strconv"
)

const (
	DEFAULT_ORDER_EVENT_ID int = 0
)

type OrderEvent struct {
	Id				int64	`json:"id"`
	Type			string	`json:"type"`
	EntityId		int64	`json:"entity_id"`
	Object			string 	`json:"object"`
	CreatedAt		string 	`json:"created_at"`
}

func (event OrderEvent) GetEventId() int {
	return int(event.Id)
}

func (event OrderEvent) GetEventType() string {
	return event.Type
}

func (event OrderEvent) GetEntityId() int {
	return int(event.EntityId)
}

func (event OrderEvent) GetEntityType() string {
	return "order"
}

func (event OrderEvent) GetJsonString() string {
	return event.Object
}

func TransferOrderEvents(dbEvent *sql.DB, dbVein *sql.DB, service Service) {
	var lastEventId int

	var id, entityId sql.NullInt64
	var eventType, object, createdAt sql.NullString

	var event OrderEvent

	for {
		err := dbVein.QueryRow("SELECT event_id FROM vein_events GROUP BY event_id ORDER BY event_id DESC LIMIT 1;").Scan(&lastEventId)
		if err != nil {
			lastEventId = DEFAULT_ORDER_EVENT_ID
		}

		rows, err := dbEvent.Query("SELECT " +
			"id, " +
			"type, " +
			"entity_id, " +
			"object, " +
			"created_at " +
			"FROM order_event " +
			"WHERE id > " + strconv.Itoa(lastEventId) + " LIMIT 1000;")
		checkErr(err)

		for rows.Next() {
			err = rows.Scan(&id, &eventType, &entityId, &object, &createdAt)
			checkErr(err)

			if id.Valid {
				event.Id = id.Int64
			} else {
				event.Id = 0
			}
			if entityId.Valid {
				event.EntityId = entityId.Int64
			} else {
				event.EntityId = 0
			}
			if eventType.Valid {
				event.Type = eventType.String
			} else {
				event.Type = ""
			}
			if object.Valid {
				event.Object = object.String
			} else {
				event.Object = ""
			}
			if createdAt.Valid {
				event.CreatedAt = createdAt.String
			} else {
				event.CreatedAt = ""
			}

			_, err = dbVein.Exec("INSERT vein_events SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, object=?",
				service.Name, event.GetEventType(), event.GetEventId(), event.GetEntityType(), event.GetEntityId(), event.GetJsonString())
			checkErr(err)
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}
