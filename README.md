simplegres
===========

WORK IN PROGRESS, NOT COMPLETE, WILL IMPLEMENT WITH hstore


-----------------------

[![Build Status](https://travis-ci.org/xyproto/simplegres.svg?branch=master)](https://travis-ci.org/xyproto/simplegres)
[![GoDoc](https://godoc.org/github.com/xyproto/simplegres?status.svg)](http://godoc.org/github.com/xyproto/simplegres)


Easy way to use a PostgreSQL database from Go.


Online API Documentation
------------------------

[godoc.org](http://godoc.org/github.com/xyproto/simplegres)


Features and limitations
------------------------

* Supports simple use of lists, hashmaps, sets and key/values.
* Deals mainly with strings.
* Uses the [mysql](https://github.com/go-sql-driver/mysql) package.
* Modeled after [simpleredis](https://github.com/xyproto/simpleredis).
* The hash maps behaves like hash maps, but are not backed by actual hashmaps, unlike with [simpleredis](https://github.com/xyproto/simpleredis). This is for keeping compatibility with simpleredis. If performance when scaling up is a concern, simpleredis backed by [redis](https://redis.io) might be a better choice.


Sample usage
------------

~~~go
package main

import (
	"log"

	"github.com/xyproto/simplegres"
)

func main() {
	// Check if the simplegres service is up
	if err := db.TestConnection(); err != nil {
		log.Fatalln("Could not connect to local database. Is the service up and running?")
	}

	// Create a Host, connect to the local db server
	host := db.New()

	// Connecting to a different host/port
	//host := db.NewHost("server:3306/db")

	// Connect to a different db host/port, with a username and password
	// host := db.NewHost("username:password@server:port/db")

	// Close the connection when the function returns
	defer host.Close()

	// Create a list named "greetings"
	list, err := db.NewList(host, "greetings")
	if err != nil {
		log.Fatalln("Could not create list!")
	}

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

A PostgreSQL Database must be up and running locally for `go test` to work.


Version, license and author
---------------------------

* Version: 2.0
* License: MIT
* Author: Alexander F Rødseth

