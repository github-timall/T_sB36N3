package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func checkErr(err error) {
	if err != nil {
		//log.Fatalf("FAILURE: %s", err.Error())
		panic(err)
	}
}

func main() {
	config := LoadConfig()

	dbEvent, err := sql.Open("mysql", config.DsnEvent)
	checkErr(err)
	defer dbEvent.Close()

	err = dbEvent.Ping()
	checkErr(err)

	dbVein, err := sql.Open("mysql", config.DsnVein)
	checkErr(err)
	defer dbVein.Close()

	err = dbVein.Ping()
	checkErr(err)

	err = InitVeinEvent(dbVein)
	checkErr(err)

	if config.ServiceName == "lead" && config.WorkerSettings.TransfersRun {
		for _, service := range config.Services {
			go TransferLeadEvents(dbEvent, dbVein, service)
		}
	}

	if config.ServiceName == "order" && config.WorkerSettings.TransfersRun {
		for _, service := range config.Services {
			go TransferOrderEvents(dbEvent, dbVein, service)
		}
	}

	veinEvents := make(chan VeinEvent)
	veinResponse := make(chan Response)

	for _, service := range config.Services {
		for _, action := range service.Actions {
			go VeinFirstEvents(dbVein, service, action, veinEvents)
			go VeinSecondEvents(dbVein, service, action, veinEvents)
			go VeinThirdEvents(dbVein, service, action, veinEvents)
			go FetchVeinEvents(veinEvents, veinResponse, service, action, config.HttpClientSettings)
		}
	}

	SaveVeinEvents(dbVein, veinResponse)
}
