package main

import (
	"database/sql"
	"net/http"
	"time"
	"log"
	"bytes"
	"io/ioutil"
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
		Object 		string
	}
)

func InitVeinEvent(db *sql.DB) (err error) {
	query := "CREATE TABLE IF NOT EXISTS `vein_events` (" +
		"`id` 			INT NOT NULL AUTO_INCREMENT," +
		"`status` 		SMALLINT NOT NULL DEFAULT 1," +
		"`service_name` VARCHAR(255) DEFAULT NULL," +

		"`event_type` 	CHAR(32) DEFAULT NULL," +
		"`event_id` 	INT NOT NULL DEFAULT 0," +

		"`entity_type` 	CHAR(32) DEFAULT NULL," +
		"`entity_id` 	INT NOT NULL DEFAULT 0," +

		"`try_success` 	TINYINT(1) DEFAULT 0," +
		"`try_number` 	SMALLINT NOT NULL DEFAULT 0," +
		"`try_time` 	DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"`try_response` TEXT DEFAULT NULL," +

		"`object` 		JSON NOT NULL," +
		"`created_at` 	DATETIME DEFAULT CURRENT_TIMESTAMP," +

		"PRIMARY KEY (`id`)," +
		"INDEX `vein_event_fetch_time` (`service_name`, `entity_type`, `event_type`, `try_success`, `try_time`, `status`)," +
		"INDEX `vein_event_fetch` (`service_name`, `entity_type`, `event_type`, `try_success`, `status`)" +
		") ENGINE='InnoDB' COLLATE 'utf8_unicode_ci';"
	_ ,err = db.Exec(query)
	return
}

func VeinFirstEvents(dbVein *sql.DB, service Service, action Action, veinEvents chan VeinEvent) {
	sub_query := "SELECT MIN(id) " +
		"FROM vein_events " +
		"WHERE service_name = ? AND entity_type = ? AND event_type = ? AND try_success = 0 AND status = 1 " +
		"GROUP BY entity_id"
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, object " +
		"FROM vein_events " +
		"WHERE id IN (" + sub_query + ") AND try_number = 0 " +
		"LIMIT 1000"

	var ServiceName, EventType, EntityType, Object sql.NullString
	var event VeinEvent

	for {
		rows, err := dbVein.Query(query, service.Name, action.EntityType, action.EventType)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType,
				&event.EntityId, &event.TrySuccess, &event.TryNumber, &Object)
			if err != nil {
				checkErr(err)
			}
			if ServiceName.Valid {
				event.ServiceName = ServiceName.String
			}
			if EventType.Valid {
				event.EventType = EventType.String
			}
			if EntityType.Valid {
				event.EntityType = EntityType.String
			}
			if Object.Valid {
				event.Object = Object.String
			}
			VeinStatusPending(dbVein, event.Id)
			veinEvents <- event
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}

func VeinSecondEvents(dbVein *sql.DB, service Service, action Action, veinEvents chan VeinEvent) {
	loc, _ := time.LoadLocation("Europe/Moscow")

	sub_query := "SELECT MIN(id) " +
		"FROM vein_events " +
		"WHERE service_name = ? AND entity_type = ? AND event_type = ? AND try_success = 0 AND try_time < ? AND status = 1 " +
		"GROUP BY entity_id"
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, object " +
		"FROM vein_events " +
		"WHERE id IN (" + sub_query + ") AND try_number = 1 " +
		"LIMIT 1000"

	var ServiceName, EventType, EntityType, Object sql.NullString
	var event VeinEvent

	for {
		DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(VEIN_SECOND_ATTEMPT)).Format("2006-01-02 15:04:05")
		rows, err := dbVein.Query(query, service.Name, action.EntityType, action.EventType, DateTime)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType,
				&event.EntityId, &event.TrySuccess, &event.TryNumber, &Object)
			if err != nil {
				checkErr(err)
			}
			if ServiceName.Valid {
				event.ServiceName = ServiceName.String
			}
			if EventType.Valid {
				event.EventType = EventType.String
			}
			if EntityType.Valid {
				event.EntityType = EntityType.String
			}
			if Object.Valid {
				event.Object = Object.String
			}
			VeinStatusPending(dbVein, event.Id)
			veinEvents <- event
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}

func VeinThirdEvents(dbVein *sql.DB, service Service, action Action, veinEvents chan <- VeinEvent) {
	loc, _ := time.LoadLocation("Europe/Moscow")

	sub_query := "SELECT MIN(id) " +
		"FROM vein_events " +
		"WHERE service_name = ? AND entity_type = ? AND event_type = ? AND try_success = 0 AND try_time < ? AND status = 1 " +
		"GROUP BY entity_id"
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, object " +
		"FROM vein_events " +
		"WHERE id IN (" + sub_query + ") AND try_number = 2 " +
		"LIMIT 1000"

	var ServiceName, EventType, EntityType, Object sql.NullString
	var event VeinEvent

	for {
		DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(VEIN_THIRD_ATTEMPT)).Format("2006-01-02 15:04:05")
		rows, err := dbVein.Query(query, service.Name, action.EntityType, action.EventType, DateTime)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType,
				&event.EntityId, &event.TrySuccess, &event.TryNumber, &Object)
			if err != nil {
				checkErr(err)
			}
			if ServiceName.Valid {
				event.ServiceName = ServiceName.String
			}
			if EventType.Valid {
				event.EventType = EventType.String
			}
			if EntityType.Valid {
				event.EntityType = EntityType.String
			}
			if Object.Valid {
				event.Object = Object.String
			}
			VeinStatusPending(dbVein, event.Id)
			veinEvents <- event
		}
		rows.Close()
		time.Sleep(time.Second)
	}
	close(veinEvents)
}

func VeinStatusPending(dbVein *sql.DB, veinEventId int) {
	_, err := dbVein.Exec("UPDATE vein_events SET status=? WHERE id=?", VEIN_STATUS_PENDING, veinEventId)
	checkErr(err)
}

func FetchVeinEvents(veinEvents <- chan VeinEvent, veinResponse chan <- Response, service Service, action Action, httpClientSettings HttpClientSettings) {
	var response Response
	client := &http.Client{
		Timeout: time.Second * httpClientSettings.Timeout,
	}

	for veinEvent := range veinEvents {
		log.Printf("VEIN_EVENT: %+v\n", veinEvent)

		req, err := http.NewRequest("POST", service.Url + action.Url, bytes.NewBufferString(veinEvent.Object))
		checkErr(err)

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer " + service.AccessToken)

		resp, err := client.Do(req)
		checkErr(err)

		response.VeinEvent = veinEvent
		response.StatusCode = resp.StatusCode

		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		response.Body = string(body)

		log.Printf("RESPONSE: %+v\n", response)
		veinResponse <- response
	}
	close(veinResponse)
}

func SaveVeinEvents(dbVein *sql.DB, veinResponses <- chan Response) {
	loc, _ := time.LoadLocation("Europe/Moscow")

	for veinResponse := range veinResponses {
		veinResponse.VeinEvent.TryNumber++

		if veinResponse.StatusCode == 200 {
			veinResponse.VeinEvent.TrySuccess = 1
		}

		tryTime := time.Now().In(loc).Format("2006-01-02 15:04:05")

		_, err := dbVein.Exec("UPDATE vein_events SET status=?, try_success=?, try_number=?, try_time=?, try_response=? WHERE id=?",
			VEIN_STATUS_READY, veinResponse.VeinEvent.TrySuccess, veinResponse.VeinEvent.TryNumber, tryTime, veinResponse.Body, veinResponse.VeinEvent.Id)
		checkErr(err)
	}
}