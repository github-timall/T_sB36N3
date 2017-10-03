package main

import (
	"database/sql"
	"strconv"
	"encoding/json"
)

type LeadEvent struct {
	Id				int64	`json:"id"`
	LeadId			int64 	`json:"lead_id"`
	Sum				float64 `json:"sum"`
	Status			string 	`json:"status"`
	ClientAge		string 	`json:"client_age"`
	ClientGender	string 	`json:"client_gender"`
	ClientRegion	string 	`json:"client_region"`
	CreatedAt		string 	`json:"created_at"`
}

func (event LeadEvent) getEventType() string {
	return "status"
}

func (event LeadEvent) getEventId() int {
	return int(event.Id)
}

func (event LeadEvent) getEntityType() string {
	return "lead"
}

func (event LeadEvent) getEntityId() int {
	return int(event.LeadId)
}

func (event LeadEvent) getJsonString() string {
	b, _ := json.Marshal(event)
	return string(b)
}

func getLeadEvents(db *sql.DB, lastEventId int) (events []LeadEvent, err error) {
	var query string

	if lastEventId == 0 {
		query = "SELECT " +
				"id, " +
				"lead_id, " +
				"c_time, " +
				"sum, " +
				"status, " +
				"client_age, " +
				"client_gender, " +
				"client_region " +
			"FROM tracking_lead_log WHERE id > 1486238;"
	} else {
		query = "SELECT " +
				"id, " +
				"lead_id, " +
				"c_time, " +
				"sum," +
				"status, " +
				"client_age, " +
				"client_gender, " +
				"client_region " +
			"FROM tracking_lead_log " +
			"WHERE id > " + strconv.Itoa(lastEventId) + ";"
	}
	var rows *sql.Rows
	rows, err = db.Query(query)
	if err != nil {
		return
	}

	var id, lead_id sql.NullInt64
	var sum sql.NullFloat64
	var create_at, status, client_age, client_gender, client_region sql.NullString

	for rows.Next() {
		var event LeadEvent
		err = rows.Scan(&id, &lead_id, &create_at, &sum, &status, &client_age, &client_gender, &client_region)
		if err != nil {
			return
		}

		if id.Valid {
			event.Id = id.Int64
		}
		if lead_id.Valid {
			event.LeadId = lead_id.Int64
		}
		if create_at.Valid {
			event.CreatedAt = create_at.String
		}
		if sum.Valid {
			event.Sum = sum.Float64
		}
		if status.Valid {
			event.Status = status.String
		}
		if client_age.Valid {
			event.ClientAge = client_age.String
		}
		if client_gender.Valid {
			event.ClientGender = client_gender.String
		}
		if client_region.Valid {
			event.ClientRegion = client_region.String
		}

		events = append(events, event)
	}
	return
}