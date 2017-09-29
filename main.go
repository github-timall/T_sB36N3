package main

import (
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

func main() {
	config, err := LoadConfig()
	checkErr(err)

	var db *sql.DB
	db, err = sql.Open("mysql", config.Db.Dsn)
	checkErr(err)
	defer db.Close()

	err = db.Ping()
	checkErr(err)

	initSendLog(db)

	err = addLeadEvents(db)
	checkErr(err)

	var events []Event
	if config.Service == "lead" {
		events, err = getLeadEventsFirst(db)
	}
	checkErr(err)

	for _, service := range config.Services {
		for _, event := range events {
			response := fetchEvent(service, event)
			err = updateLog(db, response)
			checkErr(err)
		}
	}
}

func initSendLog(db *sql.DB) {
	query := "CREATE TABLE IF NOT EXISTS `vein_send_log` (" +
		"`id` INT NOT NULL AUTO_INCREMENT," +
		"`event_id` INT NOT NULL DEFAULT 0," +
		"`entity_type` VARCHAR(255) DEFAULT 'default'," +

		"`try_success` TINYINT(1) DEFAULT 0," +
		"`try_number` SMALLINT NOT NULL DEFAULT 0," +
		"`try_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"`try_response` TEXT DEFAULT NULL," +

		"`created_at` DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"PRIMARY KEY (`id`)," +
		"UNIQUE `vein_event_entity_type` (`event_id`, `entity_type`)" +
		") ENGINE='InnoDB' COLLATE 'utf8_unicode_ci';"
	_ ,err := db.Exec(query)
	checkErr(err)
}

func fetchEvent(service Service, event Event) (response Response) {
	fmt.Printf("%+v\n", event)

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(event.Object)

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

	response.LogId = event.Id
	response.StatusCode = resp.StatusCode
	body, _ := ioutil.ReadAll(resp.Body)
	response.Body = string(body)

	fmt.Printf("%+v\n", response)

	return response
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("FAILURE: %s", err.Error())
	}
}