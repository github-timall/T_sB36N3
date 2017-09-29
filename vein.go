package main

import (
	"database/sql"
	"time"
)

type (
	Request struct {
		Id				int		`json:"id"`
		EventId			int 	`json:"event_id"`
		EntityType		string 	`json:"entity_type"`
		EntityId		int 	`json:"entity_id"`
		EntityEvent		int 	`json:"entity_event"`
		TryNumber		int 	`json:"try_number"`
		TryTime			string 	`json:"try_time"`
		CreatedAt		string 	`json:"created_at"`
	}

	Response struct {
		LogId 		int
		StatusCode 	int
		Body 		string
	}
)

func updateLog(db *sql.DB, response Response) (err error)  {
	var try_number, try_success int
	err = db.QueryRow("SELECT try_number FROM vein_send_log WHERE id = ?;", response.LogId).Scan(&try_number)
	if err != nil {
		return
	}

	try_number++

	try_success = 0
	if response.StatusCode == 200 {
		try_success = 1
	}

	var stmt *sql.Stmt
	stmt, err = db.Prepare("UPDATE vein_send_log SET try_success=?, try_number=?, try_time=?, try_response=? WHERE id=?")
	if err != nil {
		return err
	}

	loc, _ := time.LoadLocation("Europe/Moscow")
	try_time := time.Now().In(loc).Format("2006-01-02 15:04:05")

	_, err = stmt.Exec(try_success, try_number, try_time, response.Body, response.LogId)
	if err != nil {
		return err
	}
	return
}