package db

import (
	"database/sql"
 	_ "github.com/go-sql-driver/mysql"
	"log"
)

var (
	Db *sql.DB

	mainData *sql.Stmt
)

func prepareStmt(db *sql.DB, stmt string) *sql.Stmt {
	res, err := db.Prepare(stmt)
	if err != nil {
		log.Fatal("Could not prepare `" + stmt + "`: " + err.Error())
	}

	return res
}

func InitStmts(*settings) {
	mainData = prepareStmt(Db, "SELECT * FROM " + settings.Db.Table)
}