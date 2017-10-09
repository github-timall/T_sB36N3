package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"time"
	"log"
	"bytes"
	"encoding/json"
	"net/http"
	"io/ioutil"
)

func checkErr(err error) {
	if err != nil {
		//log.Fatalf("FAILURE: %s", err.Error())
		panic(err)
	}
}

func main() {
	config := LoadConfig()

	dbEvent, err := sql.Open("mysql", config.DsnEvent)
	checkErr(err)
	defer dbEvent.Close()

	err = dbEvent.Ping()
	checkErr(err)

	dbVein, err := sql.Open("mysql", config.DsnVein)
	checkErr(err)
	defer dbVein.Close()

	err = dbVein.Ping()
	checkErr(err)

	err = InitVeinEvent(dbVein)
	checkErr(err)

	if config.Service == "lead" {
		for _, service := range config.Services {
			go TransferLeadEvents(dbEvent, dbVein, service)
		}
	}

	veinEvents := make(chan VeinEvent)
	veinResponse := make(chan Response)

	for _, service := range config.Services {
		go VeinFirstEvents(dbVein, service, veinEvents)
		go VeinSecondEvents(dbVein, service, veinEvents)
		go VeinThirdEvents(dbVein, service, veinEvents)
		go FetchVeinEvents(veinEvents, veinResponse, service)
	}

	SaveVeinEvents(dbVein, veinResponse)
}

func TransferLeadEvents(dbEvent *sql.DB, dbVein *sql.DB, service Service) {
	var lastEventId int

	var id, leadId sql.NullInt64
	var sum sql.NullFloat64
	var createAt, status, client_age, client_gender, client_region sql.NullString

	var event LeadEvent

	for {
		err := dbVein.QueryRow("SELECT event_id FROM vein_events GROUP BY event_id ORDER BY event_id DESC LIMIT 1;").Scan(&lastEventId)
		if err != nil {
			lastEventId = DEFAULT_EVENT_ID
		}

		rows, err := dbEvent.Query("SELECT " +
			"id, " +
			"lead_id, " +
			"c_time, " +
			"sum," +
			"status, " +
			"client_age, " +
			"client_gender, " +
			"client_region " +
		"FROM tracking_lead_log " +
		"WHERE id > " + strconv.Itoa(lastEventId) + " LIMIT 1000;")
		checkErr(err)

		for rows.Next() {
			err = rows.Scan(&id, &leadId, &createAt, &sum, &status, &client_age, &client_gender, &client_region)
			checkErr(err)

			if id.Valid {
				event.Id = id.Int64
			} else {
				event.Id = 0
			}
			if leadId.Valid {
				event.LeadId = leadId.Int64
			} else {
				event.LeadId = 0
			}
			if createAt.Valid {
				event.CreatedAt = createAt.String
			} else {
				event.CreatedAt = ""
			}
			if sum.Valid {
				event.Sum = sum.Float64
			} else {
				event.Sum = 0.0
			}
			if status.Valid {
				event.Status = status.String
			}
			if client_age.Valid {
				event.ClientAge = client_age.String
			} else {
				event.ClientAge = ""
			}
			if client_gender.Valid {
				event.ClientGender = client_gender.String
			} else {
				event.ClientGender = ""
			}
			if client_region.Valid {
				event.ClientRegion = client_region.String
			} else {
				event.ClientRegion = ""
			}

			_, err = dbVein.Exec("INSERT vein_events SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, data=?",
				service.Name, event.GetEventType(), event.GetEventId(), event.GetEntityType(), event.GetEntityId(), event.GetJsonString())
			checkErr(err)
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}

func VeinFirstEvents(dbVein *sql.DB, service Service, veinEvents chan VeinEvent) {
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_events WHERE id IN (" +
		"SELECT MIN(id) FROM vein_events WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 AND status = 1 GROUP BY entity_id" +
		") AND try_number = 0 LIMIT 1000"

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for {
		rows, err := dbVein.Query(query, service.Name)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TryNumber, &Data)
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
			if Data.Valid {
				event.Data = Data.String
			}
			VeinStatusPending(dbVein, event.Id)
			veinEvents <- event
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}

func VeinSecondEvents(dbVein *sql.DB, service Service, veinEvents chan VeinEvent) {
	loc, _ := time.LoadLocation("Europe/Moscow")
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_events WHERE id IN (" +
		"SELECT MIN(id) FROM vein_events WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 AND try_time < ? AND status = 1 GROUP BY entity_id" +
		") AND try_number = 1 LIMIT 1000"

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for {
		DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(VEIN_SECOND_ATTEMPT)).Format("2006-01-02 15:04:05")
		rows, err := dbVein.Query(query, service.Name, DateTime)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TryNumber, &Data)
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
			if Data.Valid {
				event.Data = Data.String
			}
			VeinStatusPending(dbVein, event.Id)
			veinEvents <- event
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}

func VeinThirdEvents(dbVein *sql.DB, service Service, veinEvents chan <- VeinEvent) {
	loc, _ := time.LoadLocation("Europe/Moscow")
	query := "SELECT id, service_name, event_type, event_id, entity_type, entity_id, try_success, try_number, data FROM vein_events WHERE id IN (" +
		"SELECT MIN(id) FROM vein_events WHERE entity_type = 'lead' AND service_name = ? AND try_success = 0 AND try_time < ? AND status = 1 GROUP BY entity_id" +
		") AND try_number = 2 LIMIT 1000"

	var ServiceName, EventType, EntityType, Data sql.NullString
	var event VeinEvent

	for {
		DateTime := time.Now().In(loc).Add(- time.Minute * time.Duration(VEIN_THIRD_ATTEMPT)).Format("2006-01-02 15:04:05")
		rows, err := dbVein.Query(query, service.Name, DateTime)
		if err != nil {
			checkErr(err)
		}

		for rows.Next() {
			err = rows.Scan(&event.Id, &ServiceName, &EventType, &event.EventId, &EntityType, &event.EntityId, &event.TrySuccess, &event.TryNumber, &Data)
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
			if Data.Valid {
				event.Data = Data.String
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

func FetchVeinEvents(veinEvents <- chan VeinEvent, veinResponse chan <- Response, service Service) {
	var response Response
	b := new(bytes.Buffer)
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	for veinEvent := range veinEvents {
		log.Printf("VEIN_EVENT: %+v\n", veinEvent)

		json.NewEncoder(b).Encode(veinEvent.Data)

		req, err := http.NewRequest("POST", service.Url, b)
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