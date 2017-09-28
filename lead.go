package main

import (
	"database/sql"
	"strconv"
	"encoding/json"
)

type Lead struct {
	Id				int64	`json:"id"`
	LeadId			int64 	`json:"lead_id"`
	Sum				float64 `json:"sum"`
	Status			string 	`json:"status"`
	ClientAge		string 	`json:"client_age"`
	ClientGender	string 	`json:"client_gender"`
	ClientRegion	string 	`json:"client_region"`
	CreatedAt		string 	`json:"created_at"`
}

type Event struct {
	Id 			int
	EntityEvent int
	JsonObject 	string
}
func addLeadEvents(db *sql.DB) (err error) {
	//loc, _ := time.LoadLocation("Europe/Moscow")
	//time.Now().In(loc).Format("2006-01-02 15:04:05")

	var query string
	var eventLastId int

	err = db.QueryRow("SELECT event_id FROM vein_send_log ORDER BY event_id DESC LIMIT 1;").Scan(&eventLastId)

	if err != nil {
		query = "SELECT id, lead_id, c_time, sum, status,client_age, client_gender, client_region FROM tracking_lead_log;"
	} else {
		query = "SELECT id, lead_id, c_time, sum, status,client_age, client_gender, client_region FROM tracking_lead_log WHERE id > " + strconv.Itoa(eventLastId) + ";"
	}

	rows, err := db.Query(query)
	if err != nil {
		return err
	}

	var id, lead_id sql.NullInt64
	var sum sql.NullFloat64
	var create_at, status, client_age, client_gender, client_region sql.NullString

	for rows.Next() {
		err = rows.Scan(&id, &lead_id, &create_at, &sum, &status, &client_age, &client_gender, &client_region)
		if err != nil {
			return err
		}

		stmt, err := db.Prepare("INSERT INTO vein_send_log SET event_id=?, entity_type=?, entity_id=?, entity_event=?")
		if err != nil {
			return err
		}

		_, err = stmt.Exec(&id, "lead", &lead_id, 1)
		if err != nil {
			return err
		}
	}
	return err
}

func getLeadEventsFirst(db *sql.DB) (events []Event, err error) {
	query := "SELECT " +
			"vein.id as vein_id," +
			"vein.entity_event as vein_entity_event," +
			"entity.id," +
			"entity.lead_id," +
			"entity.c_time," +
			"entity.sum," +
			"entity.status," +
			"entity.client_age," +
			"entity.client_gender," +
			"entity.client_region " +
		"FROM tracking_lead_log entity LEFT JOIN vein_send_log vein ON vein.event_id = entity.id " +
		"WHERE try_number = ?"
	rows, err := db.Query(query, 0)
	if err != nil {
		return
	}

	var vein_id, vein_entity_event int
	var id, lead_id sql.NullInt64
	var sum sql.NullFloat64
	var create_at, status, client_age, client_gender, client_region sql.NullString

	for rows.Next() {
		var lead Lead
		var event Event
		err = rows.Scan(&vein_id, &vein_entity_event, &id, &lead_id, &create_at, &sum, &status, &client_age, &client_gender, &client_region)
		if err != nil {
			return
		}

		if id.Valid {
			lead.Id = id.Int64
		}
		if lead_id.Valid {
			lead.LeadId = lead_id.Int64
		}
		if create_at.Valid {
			lead.CreatedAt = create_at.String
		}
		if sum.Valid {
			lead.Sum = sum.Float64
		}
		if status.Valid {
			lead.Status = status.String
		}
		if client_age.Valid {
			lead.ClientAge = client_age.String
		}
		if client_gender.Valid {
			lead.ClientGender = client_gender.String
		}
		if client_region.Valid {
			lead.ClientRegion = client_region.String
		}
		event.Id = vein_id
		event.EntityEvent = vein_entity_event
		jsonData, _ := json.Marshal(lead)
		event.JsonObject = string(jsonData)
		events = append(events, event)
	}
	return
}