package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"strconv"
)

var (
	dbEvent *sql.DB
	dbVein *sql.DB
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

	dbVein, err = sql.Open("mysql", config.DsnVein)
	checkErr(err)
	defer dbVein.Close()

	err = dbVein.Ping()
	checkErr(err)

	err = InitSendLog(dbVein)
	checkErr(err)

	if config.Service == "lead" {
		for _, service := range config.Services {
			TransferLeadEvents(service)
		}
	}

	//var response Response
	//
	//for {
	//	time.Sleep(time.Second)
	//
	//	for _, service := range config.Services {
	//		veinFirstEvents, err := GetFirstEvents(dbVein, service)
	//		checkErr(err)
	//		for _, veinFirstEvent := range veinFirstEvents {
	//			response = FetchEvent(veinFirstEvent, service)
	//			UpdateEvent(dbVein, response)
	//		}
	//	}
	//
	//	for _, service := range config.Services {
	//		veinSecondEvents, err := GetSecondEvents(dbVein, service)
	//		checkErr(err)
	//		for _, veinSecondEvent := range veinSecondEvents {
	//			response = FetchEvent(veinSecondEvent, service)
	//			UpdateEvent(dbVein, response)
	//		}
	//	}
	//
	//	for _, service := range config.Services {
	//		veinThirdEvents, err := GetThirdEvents(dbVein, service)
	//		checkErr(err)
	//		for _, veinThirdEvent := range veinThirdEvents {
	//			response = FetchEvent(veinThirdEvent, service)
	//			UpdateEvent(dbVein, response)
	//		}
	//	}
	//}
}

func TransferLeadEvents(service Service) {
	var lastEventId int

	var id, leadId sql.NullInt64
	var sum sql.NullFloat64
	var createAt, status, client_age, client_gender, client_region sql.NullString

	var event LeadEvent

	//for {
		err := dbEvent.QueryRow("SELECT event_id FROM vein_send_log GROUP BY event_id ORDER BY event_id DESC LIMIT 1;").Scan(lastEventId)
		if err != nil {
			lastEventId = 1492666
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
			err := rows.Scan(&id, &leadId, &createAt, &sum, &status, &client_age, &client_gender, &client_region)
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

			_, err = dbVein.Exec("INSERT vein_send_log SET service_name=?, event_type=?, event_id=?, entity_type=?, entity_id=?, data=?",
				service.Name, event.GetEventType(), event.GetEventId(), event.GetEntityType(), event.GetEntityId(), event.GetJsonString())
			checkErr(err)
		}
		rows.Close()
		time.Sleep(time.Second)
	//}
}

func LeadEvents(){

}

func SaveLeadEventsVein(){

}

func VeinEvents(){

}

func FetchVeinEvents(){

}

func SaveVeinEvents(){

}