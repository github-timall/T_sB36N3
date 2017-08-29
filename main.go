package main

import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/github-timall/T_sB36N3/config"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("FAILURE: %s", err.Error())
	}

	db, err := sql.Open("mysql", settings.Db.Dsn)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	rows, err := db.Query(fmt.Sprintf("SELECT id FROM %s WHERE c_time > '%s'", settings.Db.Table, "2017-08-22 12:09:48"))
	if err != nil {
		log.Fatal(err)
	}
	type Row struct {
		Id int
	}
	var changes []*Row
	for rows.Next() {
		//row := Row{}
		row := new(Row)
		if err := rows.Scan(&row.Id); err != nil {
			log.Fatal(err)
		}
		changes = append(changes, row)
		//fmt.Printf("%#v\n", row)
		fmt.Printf("%d\n", row.Id)
	}
	fmt.Printf("%#v\n", changes)
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

}