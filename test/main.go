package main

import (
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "go:go@/")
	fmt.Println(db)
	fmt.Println(err)

	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

    // Create the main database
	if _, err = db.Exec("CREATE DATABASE IF NOT EXISTS main CHARACTER SET = utf8"); err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

	// Open doesn't open a connection. Validate DSN data:
	if db.Ping() != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Use the main database
	if _, err := db.Exec("USE main"); err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

    // Create a numbers table
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS numbers (number INT)"); err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

}
