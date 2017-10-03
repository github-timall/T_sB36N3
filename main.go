package main

import (
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	config, err := LoadConfig()
	checkErr(err)

	db_event, err := sql.Open("mysql", config.DsnEvent)
	checkErr(err)
	db_vein, err := sql.Open("mysql", config.DsnVein)
	checkErr(err)

	defer db_event.Close()
	defer db_vein.Close()

	err = db_event.Ping()
	checkErr(err)
	err = db_vein.Ping()
	checkErr(err)

	err = initSendLog(db_vein)
	checkErr(err)

	//for {
	//	time.Sleep(time.Second)

		lastEventId := getLastEventLogId(db_vein)

		if config.Service == "lead" {
			var events []LeadEvent
			events, err = getLeadEvents(db_event, lastEventId)
			checkErr(err)
			for _, event := range events {
				for _, service := range config.Services {
					err = addLog(db_vein, event, service)
					checkErr(err)
				}
			}
		}
	//}
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("FAILURE: %s", err.Error())
	}
}