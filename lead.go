package main

import (
	"encoding/json"
)

const (
	DEFAULT_EVENT_ID int = 1502288
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