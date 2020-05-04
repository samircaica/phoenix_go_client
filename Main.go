package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/apache/calcite-avatica-go"
)

func main() {
	var (
		empID int
		dept  string
	)

	db, err := sql.Open("avatica", "http://c334-node5:8765/?authentication=SPNEGO&principal=hbase-c334@HWX.COM&keytab=/Users/scaica/hbase_conf_Samir/c334-node5/hbase.headless.keytab&krb5Conf=/etc/krb5.conf")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS EMP3")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Table EMP3 Drop")
	}

	DEPTs := []string{
		"SALES",
		"SUPPORT",
		"DEVELOPMENT",
		"MANAGEMENT",
	}

	_, err = db.Exec("CREATE TABLE EMP3 (emp_id integer not null, dept char(15) constraint PK PRIMARY KEY(EMP_ID))")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("Table EMP3 created")
	}

	//UPSERT INTO EMP3(EMP_ID,DEPT) VALUES(1,'SALES');

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("UPSERT INTO EMP3 (EMP_ID,DEPT) VALUES(?,?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close() // danger!
	for i := 0; i < 10; i++ {
		rand.Seed(time.Now().UnixNano())
		choosenDept := DEPTs[rand.Intn(len(DEPTs))]
		fmt.Println("Randomly selected this Dept : ", choosenDept)
		_, err = stmt.Exec(i+1, choosenDept)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM EMP3")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&empID, &dept)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(empID, dept)
	}
	err = rows.Err()

	defer db.Close()

}
