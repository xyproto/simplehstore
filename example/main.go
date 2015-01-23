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

	// Use instead for testing if a different host/port is up.
	// db.TestConnection("localhost:1234")

	// Create a Host, connect to the given db server
	host := db.New()

	// Use this for connecting to a different db host/port
	// host := db.NewHost("localhost:3306")

	// For connecting to a different db host/port, with a password
	// host := db.NewHost("password@dbhost:3306")

	// Close the connection host right after this function returns
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
