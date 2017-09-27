package main

import "database/sql"

type Lead struct {
	Id				int		`json:"id"`
	LeadId			int 	`json:"lead_id"`
	CTime			string 	`json:"created_at"`
	Sum				float32 `json:"sum"`
	Status			sql.NullString 	`json:"status"`
	ClientAge		sql.NullString 	`json:"client_age"`
	ClientGender	sql.NullString 	`json:"client_gender"`
	ClientRegion	sql.NullString 	`json:"client_region"`
}

func getLeadEvents(db *sql.DB) (leads []Lead, err error) {
	rows, err := db.Query("SELECT id, lead_id, c_time, sum, status,client_age, client_gender, client_region FROM tracking_lead_log LIMIT 10;")
	if err != nil {
		return
	}
	var lead Lead
	for rows.Next() {
		err = rows.Scan(&lead.Id, &lead.LeadId, &lead.CTime, &lead.Sum, &lead.Status, &lead.ClientAge, &lead.ClientGender, &lead.ClientRegion)
		if err != nil {
			return
		}
		leads = append(leads, lead)
	}
	return
}