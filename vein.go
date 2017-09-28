package main

type senLog struct {
	Id				int		`json:"id"`
	EventId			int 	`json:"event_id"`
	EntityType		string 	`json:"entity_type"`
	EntityId		int 	`json:"entity_id"`
	EntityEvent		int 	`json:"entity_event"`
	TryNumber		int 	`json:"try_number"`
	TryTime			string 	`json:"try_time"`
	CreatedAt		string 	`json:"created_at"`
}