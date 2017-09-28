package main

import (
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

func main() {
	config, err := LoadConfig()
	checkErr(err)

	db, err := sql.Open("mysql", config.Db.Dsn)
	checkErr(err)
	defer db.Close()

	err = db.Ping()
	checkErr(err)

	initSendLog(db)

	events, err := getLeadEventsFirst(db)
	checkErr(err)

	fmt.Printf("%+v\n", events)

	err = addLeadEvents(db)
	checkErr(err)
}

func initSendLog(db *sql.DB) {
	query := "CREATE TABLE IF NOT EXISTS `vein_send_log` (" +
		"`id` INT NOT NULL AUTO_INCREMENT," +
		"`event_id` INT NOT NULL DEFAULT 0," +
		"`entity_type` VARCHAR(255) DEFAULT 'default'," +
		"`entity_id` INT NOT NULL DEFAULT 0," +
		"`entity_event` INT NOT NULL DEFAULT 0," +
		"`try_number` SMALLINT NOT NULL DEFAULT 0," +
		"`try_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"`try_response` TEXT DEFAULT NULL," +
		"`created_at` DATETIME DEFAULT CURRENT_TIMESTAMP," +
		"PRIMARY KEY (`id`)" +
		") ENGINE='InnoDB' COLLATE 'utf8_unicode_ci';"
	_ ,err := db.Exec(query)
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("FAILURE: %s", err.Error())
	}
}