package main

import (
	"encoding/json"
	"database/sql"
	"strconv"
	"time"
)

const (
	DEFAULT_LEAD_EVENT_ID int = 1502288
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
	b, err := json.Marshal(event)
	checkErr(err)
	return string(b)
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
			lastEventId = DEFAULT_LEAD_EVENT_ID
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

			_, err = dbVein.Exec("INSERT vein_events SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, object=?",
				service.Name, event.GetEventType(), event.GetEventId(), event.GetEntityType(), event.GetEntityId(), event.GetJsonString())
			checkErr(err)
		}
		rows.Close()
		time.Sleep(time.Second)
	}
}