package main

import (
	"database/sql"
	//"time"
	//"log"
	//"bytes"
	//"encoding/json"
	//"net/http"
	//"io/ioutil"
)

type (
	Event interface {
		getEventType() string
		getEventId() int

		getEntityType() string
		getEntityId() int

		getJsonString() string
	}

	Response struct {
		VeinLogId 	int
		StatusCode 	int
		Body 		string
	}

 	VeinLog struct {
		Id 			int
		Object 		interface{}
	}
)

func initSendLog(db *sql.DB) (err error) {
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

func getLastEventLogId(db *sql.DB) (eventLastId int) {
	db.QueryRow("SELECT event_id FROM vein_send_log GROUP BY event_id ORDER BY event_id DESC LIMIT 1;").Scan(&eventLastId)
	return eventLastId
}

func addLog(db *sql.DB, event Event, service Service) (err error) {
	stmt, err := db.Prepare("INSERT vein_send_log SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, data=?")
	checkErr(err)

	_, err = stmt.Exec(service.Name, event.getEventType(), event.getEventId(), event.getEntityType(), event.getEntityId(), event.getJsonString())
	return
}

//func fetchEvent(vein_log VeinLog, service Service) (response Response) {
//	log.Printf("VEIN_LOG: %+v\n", vein_log)
//
//	b := new(bytes.Buffer)
//	json.NewEncoder(b).Encode(vein_log.Object)
//
//	req, err := http.NewRequest("POST", service.Url, b)
//	checkErr(err)
//	req.Header.Set("Content-Type", "application/json")
//	req.Header.Set("Authorization", "Bearer " + service.AccessToken)
//
//	client := &http.Client {
//		Timeout: time.Second * 10,
//	}
//	resp, err := client.Do(req)
//	checkErr(err)
//	defer resp.Body.Close()
//
//	response.VeinLogId = vein_log.Id
//	response.StatusCode = resp.StatusCode
//	body, _ := ioutil.ReadAll(resp.Body)
//	response.Body = string(body)
//
//	log.Printf("RESPONSE: %+v\n", response)
//
//	return response
//}
//
//func updateLog(db *sql.DB, response Response) (err error) {
//	var try_number, try_success int
//	err = db.QueryRow("SELECT try_number FROM vein_send_log WHERE id = ?;", response.VeinLogId).Scan(&try_number)
//	if err != nil {
//		return
//	}
//
//	try_number++
//
//	try_success = 0
//	if response.StatusCode == 200 {
//		try_success = 1
//	}
//
//	var stmt *sql.Stmt
//	stmt, err = db.Prepare("UPDATE vein_send_log SET try_success=?, try_number=?, try_time=?, try_response=? WHERE id=?")
//	if err != nil {
//		return err
//	}
//
//	loc, _ := time.LoadLocation("Europe/Moscow")
//	try_time := time.Now().In(loc).Format("2006-01-02 15:04:05")
//
//	_, err = stmt.Exec(try_success, try_number, try_time, response.Body, response.VeinLogId)
//	if err != nil {
//		return err
//	}
//	return
//}