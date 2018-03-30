package main

import (
	"fmt"

	"github.com/dazheng/gohive"
)

func main() {
	conn, err := gohive.Connect("localhost:10000", gohive.DefaultOptions)
	if err != nil {
		fmt.Errorf("Connect error %v", err)
	}

	fmt.Print("Set hive environment")
	_, err = conn.Exec("set hive.support.concurrency = true")
	if err != nil {
		fmt.Errorf("Connection.Exec error: %v", err)
		return
	}

	fmt.Println("create table")
	_, err = conn.Exec("create table if not exists t(c1 int)")
	if err != nil {
		fmt.Errorf("Connection.Exec error: %v", err)
		return
	}

	fmt.Println("insert into table")
	_, err = conn.Exec(" insert into default.t values(1), (2)")
	if err != nil {
		fmt.Errorf("Connection.Exec error: %v", err)
		return
	}

	fmt.Println("query table")
	rs, err := conn.Query("select c1 from t limit 10")
	if err != nil {
		fmt.Errorf("Connection.Query error: %v", err)
		return
	}
	var c1 int
	for rs.Next() {
		rs.Scan(&c1)
		fmt.Println(c1)
	}
	conn.Close()
	fmt.Println("Done")
}
