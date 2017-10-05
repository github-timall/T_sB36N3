package main

import (
	"database/sql"
	"log"
	"bytes"
	"encoding/json"
	"net/http"
	"time"
	"io/ioutil"
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

func InitSendLog(db *sql.DB) (err error) {
	query := "CREATE TABLE IF NOT EXISTS `vein_send_log` (" +
		"`id` 			INT NOT NULL AUTO_INCREMENT," +
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

func GetLastEventLogId(db *sql.DB) (int) {
	eventLastId := 0
	db.QueryRow("SELECT event_id FROM vein_send_log GROUP BY event_id ORDER BY event_id DESC LIMIT 1;").Scan(&eventLastId)
	return eventLastId
}

func AddLeadLog(db *sql.DB, event LeadEvent, service *Service) (error) {
	_, err := db.Exec("INSERT vein_send_log SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, data=?",
		service.Name, event.GetEventType(), event.GetEventId(), event.GetEntityType(), event.GetEntityId(), event.GetJsonString())
	return err
}

func GetFirstEvents(db *sql.DB, service *Service) ([]VeinEvent, error) {
	var events []VeinEvent

	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_send_log WHERE id IN (" +
		"SELECT MIN(id) FROM vein_send_log WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 GROUP BY entity_id" +
	") AND try_number = 0 LIMIT 1000"

	rows, err := db.Query(query, service.Name)
	if err != nil {
		return events, err
	}

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for rows.Next() {

		err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TrySuccess, &Data)
		if err != nil {
			return events, err
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
		if Data.Valid {
			event.Data = Data.String
		}
		events = append(events, event)
	}
	return events, nil
}

func GetSecondEvents(db *sql.DB, service *Service) ([]VeinEvent, error) {
	var events []VeinEvent

	loc, _ := time.LoadLocation("Europe/Moscow")
	DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(5)).Format("2006-01-02 15:04:05")

	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_send_log WHERE id IN (" +
		"SELECT MIN(id) FROM vein_send_log WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 AND try_time < ? GROUP BY entity_id" +
	") AND try_number = 1 LIMIT 1000"

	rows, err := db.Query(query, service.Name, DateTime)
	if err != nil {
		return events, err
	}

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for rows.Next() {
		err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TrySuccess, &Data)
		if err != nil {
			return events, err
		}
		if ServiceName.Valid {
			event.ServiceName = ServiceName.String
		} else {
			event.ServiceName = ""
		}
		if EventType.Valid {
			event.EventType = EventType.String
		} else {
			event.EventType = ""
		}
		if EntityType.Valid {
			event.EntityType = EntityType.String
		} else {
			event.EntityType = ""
		}
		if Data.Valid {
			event.Data = Data.String
		} else {
			event.Data = ""
		}
		events = append(events, event)
	}
	return events, nil
}

func GetThirdEvents(db *sql.DB, service *Service) ([]VeinEvent, error) {
	var events []VeinEvent

	loc, _ := time.LoadLocation("Europe/Moscow")
	DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(30)).Format("2006-01-02 15:04:05")

	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_send_log WHERE id IN (" +
		"SELECT MIN(id) FROM vein_send_log WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 AND try_time < ? GROUP BY entity_id" +
	") AND try_number = 2 LIMIT 1000"

	rows, err := db.Query(query, service.Name, DateTime)
	if err != nil {
		return events, err
	}

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for rows.Next() {
		err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TrySuccess, &Data)
		if err != nil {
			return events, err
		}
		if ServiceName.Valid {
			event.ServiceName = ServiceName.String
		} else {
			event.ServiceName = ""
		}
		if EventType.Valid {
			event.EventType = EventType.String
		} else {
			event.EventType = ""
		}
		if EntityType.Valid {
			event.EntityType = EntityType.String
		} else {
			event.EntityType = ""
		}
		if Data.Valid {
			event.Data = Data.String
		} else {
			event.Data = ""
		}
		events = append(events, event)
	}
	return events, nil
}

func FetchEvent(VeinEvent VeinEvent, service *Service) Response {
	log.Printf("VEIN_LOG: %+v\n", VeinEvent)

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(VeinEvent.Data)

	req, err := http.NewRequest("POST", service.Url, b)
	checkErr(err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer " + service.AccessToken)

	client := &http.Client {
		Timeout: time.Second * 10,
	}
	resp, err := client.Do(req)
	checkErr(err)
	defer resp.Body.Close()

	var response Response
	response.VeinEvent = VeinEvent
	response.StatusCode = resp.StatusCode
	body, _ := ioutil.ReadAll(resp.Body)
	response.Body = string(body)

	log.Printf("RESPONSE: %+v\n", response)

	return response
}

func UpdateEvent(db *sql.DB, response Response) (error) {
	response.VeinEvent.TryNumber++

	if response.StatusCode == 200 {
		response.VeinEvent.TrySuccess = 1
	}

	loc, _ := time.LoadLocation("Europe/Moscow")
	TryTime := time.Now().In(loc).Format("2006-01-02 15:04:05")

	log.Printf("RESPONSE IN UPDATE: %+v\n", response)

	_, err := db.Exec("UPDATE vein_send_log SET try_success=?, try_number=?, try_time=?, try_response=? WHERE id=?",
		response.VeinEvent.TrySuccess, response.VeinEvent.TryNumber, TryTime, response.Body, response.VeinEvent.Id)
	if err != nil {
		return err
	}

	return nil
}