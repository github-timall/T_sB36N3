package main

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
	"time"
)

var db *sql.DB
var err error

func main() {
	db, err = sql.Open("mysql", "root:123456@/api_drcash?charset=utf8")
	checkErr(err)
	defer db.Close()

	err = db.Ping()
	checkErr(err)

	//transferUser()
	//fmt.Print("\n")
	//transferUserInfo()
	//fmt.Print("\n")
	//transferAffiliate()
	//fmt.Print("\n")
	//transferUnique()
	//fmt.Print("\n")
	//transferDevice()
	//fmt.Print("\n")
	//transferReferrer()
	//fmt.Print("\n")
	//transferVisit()
	//fmt.Print("\n")
	//transferIp()
	//fmt.Print("\n")
	//transferClick()
	//fmt.Print("\n")
	transferIpRelation()
}

func transferUser() {
	fmt.Println("Truncate user")
	_, err := db.Exec("TRUNCATE TABLE zzr_user;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert user")
	var query string = "INSERT zzr_user (id, login, password_hash, status, auth_key, password_reset_token, created_at, updated_at) " +
		"SELECT n_id_user, login, IFNULL(password_hash, MD5(n_id_user+RAND())), status_user, MD5(n_id_user+RAND()), MD5(n_id_user+RAND()), IFNULL(created_at, NOW()), IFNULL(updated_at, NOW()) FROM tracking_users;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferUserInfo() {
	fmt.Println("Truncate user info")
	_, err := db.Exec("TRUNCATE TABLE zzr_user_info;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert user info")
	var query string = "INSERT zzr_user_info (user_id, username, skype, language, utm_source, created_at, updated_at) " +
		"SELECT n_id_user, login, skype, language, utm_source, NOW(), NOW() FROM tracking_users;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferAffiliate() {
	fmt.Println("Truncate affiliate")
	_, err := db.Exec("TRUNCATE TABLE zzr_affiliate;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert affiliate")
	var query string = "INSERT zzr_affiliate (id, user_id, name, team_id, created_at, updated_at) " +
		"SELECT	id_affiliate, (SELECT n_id_user	FROM tracking_users WHERE affiliate_id = tracking_affiliates.id_affiliate) as n_id_user, name_affiliate, team_id, NOW(), NOW() FROM tracking_affiliates;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferUnique() {
	fmt.Println("Truncate unique")
	_, err := db.Exec("TRUNCATE TABLE zzr_unique;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert unique")
	var query string = "INSERT zzr_unique (id, created_at, updated_at) " +
		"SELECT id, IFNULL(created_at, NOW()), NOW() FROM tracking_unique;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferDevice() {
	fmt.Println("Truncate device")
	_, err := db.Exec("TRUNCATE TABLE zzr_device;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert device")
	var query string = "INSERT zzr_device (id, is_mobile, is_tablet, is_bot, is_desktop, os, os_version, client_type, client_name, client_version, brand, model, created_at, updated_at) " +
		"SELECT id, is_mobile, is_tablet, is_bot, is_desktop, os, os_version, client_type, client_name, client_version, brand, model, NOW(), NOW() FROM tracking_device;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferReferrer() {
	fmt.Println("Truncate referrer")
	_, err := db.Exec("TRUNCATE TABLE zzr_referrer;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert referrer")
	var query string = "INSERT zzr_referrer (id, host, url, created_at, updated_at)	" +
		"SELECT id, host, host, NOW(), NOW() FROM tracking_refhost;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferVisit() {
	fmt.Println("Delete visit")
	_, err := db.Exec("DELETE FROM zzr_visit;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert visit")
	var query string = "INSERT zzr_visit (id, parent_id, type, unique_id, device_id, referrer_id, geo_code, user_agent, headers, created_at, updated_at) " +
		"SELECT id, 0, 1, unique_id, " +
		"(SELECT device_id FROM tracking_unique WHERE tracking_unique.id = tracking_visit.unique_id LIMIT 1) as device_id, " +
		"referer_id, geo_code, null, null, created_at, NOW() FROM tracking_visit;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferIp() {
	fmt.Println("Delete ip")
	_, err := db.Exec("DELETE FROM zzr_ip;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert ip")
	var query string = "INSERT zzr_ip (id, ip, geo_code, city, created_at, updated_at) " +
		"SELECT id, ip, geo_code, city, NOW(), NOW() FROM tracking_ip;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferClick() {
	fmt.Println("Delete click")
	_, err := db.Exec("DELETE FROM zzr_click;")
	checkErr(err)
	fmt.Println("OK")

	fmt.Println("Insert click")
	var query string = "INSERT zzr_click (id, visit_id, method, created_at, updated_at) " +
		"SELECT id, visit_id, method, created_at, NOW() FROM tracking_click;"
	res, err := db.Exec(query)
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Printf("RowsAffected: %d \n", affect)
	fmt.Println("OK")
}

func transferIpRelation() {
	p := fmt.Printf

	p("Delete ip visit relation %s \n", time.Now())
	_, err := db.Exec("DELETE FROM zzr_ip_to_visit;")
	checkErr(err)
	p("Delete ip click relation \n")
	_, err = db.Exec("DELETE FROM zzr_ip_to_click;")
	checkErr(err)
	p("OK %s \n", time.Now())

	p("Insert relation %s \n", time.Now())
	rows, err := db.Query("SELECT id, visit_id, ip_id, method, created_at FROM tracking_click;")
	checkErr(err)

	type ClickRow struct {
		id         int
		visit_id int
		ip_id  int
		method int
		created_at string
	}

	var click ClickRow

	for rows.Next() {
		err = rows.Scan(&click.id, &click.visit_id, &click.ip_id, &click.method, &click.created_at)
		checkErr(err)
		stmt, err := db.Prepare("INSERT INTO zzr_ip_to_click (click_id, ip_id) values(?,?);")
		checkErr(err)
		_, err = stmt.Exec(click.id, click.ip_id)
		checkErr(err)

		stmt, err = db.Prepare("INSERT INTO zzr_ip_to_visit (visit_id, ip_id) values(?,?);")
		checkErr(err)
		_, err = stmt.Exec(click.visit_id, click.ip_id)
		checkErr(err)
	}

	fmt.Println(click)
	p("OK %s \n", time.Now())
}

func checkErr(err error) {
	if (err != nil) {
		panic(err)
	}
}