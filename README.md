# Work in progress. Tests currently fail.

db
==

[![Build Status](https://travis-ci.org/xyproto/db.svg?branch=master)](https://travis-ci.org/xyproto/db)
[![GoDoc](https://godoc.org/github.com/xyproto/db?status.svg)](http://godoc.org/github.com/xyproto/db)


Easy way to use a MariaDB/MySQL database from Go. Modeled after [simpleredis](https://github.com/xyproto/simpleredis).


Online API Documentation
------------------------

[godoc.org](http://godoc.org/github.com/xyproto/db)


Features and limitations
------------------------

* Supports simple use of lists, hashmaps, sets and key/values.
* Deals mainly with strings.
* Uses the [mysql](https://github.com/go-sql-driver/mysql) package.


Sample usage
------------

~~~go
package main

import (
	"log"

	"github.com/xyproto/db"
)

func main() {
	// Check if the db service is up
	if err := db.TestConnection(); err != nil {
		log.Fatalln("Could not connect to database. Is the service up and running?")
	}

	// Create a Host, connect to the local db server
	host := db.New()

	// Connecting to a different host/port
	//host := db.NewHost("server:3306/db")

	// Connect to a different db host/port, with a username and password
	// host := db.NewHost("username:password@server/db")

	// Close the connection when the function returns
	defer host.Close()

	// Create a list named "greetings"
	list := db.NewList(host, "greetings")

	// Add "hello" to the list, check if there are errors
	if list.Add("hello") != nil {
		log.Fatalln("Could not add an item to list!")
	}

	// Get the last item of the list
	if item, err := list.GetLast(); err != nil {
		log.Fatalln("Could not fetch the last item from the list!")
	} else {
		log.Println("The value of the stored item is:", item)
	}

	// Remove the list
	if list.Remove() != nil {
		log.Fatalln("Could not remove the list!")
	}
}
~~~

Testing
-------

A MariaDB/MySQL Database must be up and running locally for `go test` to work.


Version, license and author
---------------------------

* Version: 1.0
* License: MIT
* Author: Alexander F Rødseth

