simplehstore
===========

[![Build Status](https://travis-ci.org/xyproto/simplehstore.svg?branch=master)](https://travis-ci.org/xyproto/simplehstore)
[![GoDoc](https://godoc.org/github.com/xyproto/simplehstore?status.svg)](http://godoc.org/github.com/xyproto/simplehstore)


Easy way to use a PostgreSQL database (and the HSTORE feature) from Go.


Online API Documentation
------------------------

[godoc.org](http://godoc.org/github.com/xyproto/simplehstore)


Known bugs
----------

* There are problems when using the HashMap. The test at [permissiongres](https://github.com/xyproto/permissiongres) currently fails.


Features and limitations
------------------------

* Requires PostgreSQL 9.1 or later.
* Supports simple use of lists, hashmaps, sets and key/values.
* Deals mainly with strings.
* Uses the [pq](https://github.com/lib/pq) package.
* Modeled after [simpleredis](https://github.com/xyproto/simpleredis).
* Uses the HSTORE for the KeyValue and HashMap types.
* Uses regular SQL for the List and Set types.
* If performance is a concern, simpleredis backed by [redis](https://redis.io) might be a better choice.

Sample usage
------------

~~~go
package main

import (
	"log"

	db "github.com/xyproto/simplehstore"
)

func main() {
	// Check if the db service is up
	if err := db.TestConnection(); err != nil {
		log.Fatalln("Could not connect to local database. Is the service up and running?")
	}

	// Create a Host, connect to the local db server
	host := db.New()

	// Connecting to a different host/port
	//host := db.NewHost("server:5432/db")

	// Connect to a different db host/port, with a username and password
	// host := db.NewHost("username:password@server/db")

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

* A PostgreSQL server must be up and running locally for `go test` to work.


License, author and version
---------------------------

* License: MIT
* Author: Alexander F Rødseth <xyproto@archlinux.org>
* Version: 2.0


