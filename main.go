package main

import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	//"encoding/json"
	"encoding/json"
)

func main() {
	config, err := LoadConfig()
	checkErr(err)

	db, err := sql.Open("mysql", config.Db.Dsn)
	checkErr(err)
	defer db.Close()

	err = db.Ping()
	checkErr(err)

	data, err := getLeadEvents(db)
	checkErr(err)

	dataJson, err := json.Marshal(data)
	checkErr(err)

	fmt.Println(string(dataJson))
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("FAILURE: %s", err.Error())
	}
}