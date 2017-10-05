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

func (event LeadEvent) GetEventType() string {
	return "status"
}

func (event LeadEvent) GetEventId() int {
	return int(event.Id)
}

func (event LeadEvent) GetEntityType() string {
	return "lead"
}

func (event LeadEvent) GetEntityId() int {
	return int(event.LeadId)
}

func (event LeadEvent) GetJsonString() string {
	b, _ := json.Marshal(event)
	return string(b)
}

func GetLeadEvents(db *sql.DB, lastEventId int) ([]LeadEvent, error) {
	var events []LeadEvent

	if lastEventId == 0 {
		lastEventId = 1492666
	}

	query := "SELECT " +
		"id, " +
		"lead_id, " +
		"c_time, " +
		"sum," +
		"status, " +
		"client_age, " +
		"client_gender, " +
		"client_region " +
	"FROM tracking_lead_log " +
	"WHERE id > " + strconv.Itoa(lastEventId) + " LIMIT 1000;"

	rows, err := db.Query(query)
	if err != nil {
		return events, err
	}

	var id, leadId sql.NullInt64
	var sum sql.NullFloat64
	var createAt, status, client_age, client_gender, client_region sql.NullString

	for rows.Next() {
		var event LeadEvent
		err := rows.Scan(&id, &leadId, &createAt, &sum, &status, &client_age, &client_gender, &client_region)
		if err != nil {
			return events, err
		}

		if id.Valid {
			event.Id = id.Int64
		}
		if leadId.Valid {
			event.LeadId = leadId.Int64
		}
		if createAt.Valid {
			event.CreatedAt = createAt.String
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
	rows.Close()
	return events, nil
}