package simplehstore

import (
	"bytes"
	"log"
	"strconv"
	"strings"
)

// Verbose can be set to true when testing, for more information
var (
	Verbose = false
)

/* --- Helper functions --- */

// twoFields splits a string into two parts, given a delimiter.
// If it works out, the two parts are returned, together with "true".
// The delimiter must exist exactly once.
func twoFields(s, delim string) (string, string, bool) {
	if strings.Count(s, delim) != 1 {
		return s, "", false
	}
	fields := strings.Split(s, delim)
	return fields[0], fields[1], true
}

// leftOf returns the string to the left of the given delimiter
func leftOf(s, delim string) string {
	if left, _, ok := twoFields(s, delim); ok {
		return strings.TrimSpace(left)
	}
	return ""
}

// rightOf returns the string to the right of the given delimiter
func rightOf(s, delim string) string {
	if _, right, ok := twoFields(s, delim); ok {
		return strings.TrimSpace(right)
	}
	return ""
}

// Parse a DSN
func splitConnectionString(connectionString string) (string, string, bool, string, string, string) {
	var (
		userPass, hostPortDatabase, dbname, hostPort,
		password, username, port, host string
		hasPassword bool
	)

	// Gather the fields

	// Optional left side of @ with username and password
	userPass = leftOf(connectionString, "@")
	if userPass != "" {
		hostPortDatabase = rightOf(connectionString, "@")
	} else {
		hostPortDatabase = strings.TrimRight(connectionString, "@")
	}
	// Optional right side of / with database name
	dbname = rightOf(hostPortDatabase, "/")
	if dbname != "" {
		hostPort = leftOf(hostPortDatabase, "/")
	} else {
		hostPort = strings.TrimRight(connectionString, "/")
		dbname = defaultDatabaseName
	}
	// Optional right side of : with password
	password = rightOf(userPass, ":")
	if password != "" {
		username = leftOf(userPass, ":")
	} else {
		if strings.HasSuffix(userPass, ":") {
			username = userPass[:len(userPass)-1]
			hasPassword = true
		} else {
			username = userPass
		}
	}
	// Optional right side of : with port
	port = rightOf(hostPort, ":")
	if port != "" {
		host = leftOf(hostPort, ":")
	} else {
		host = strings.TrimRight(hostPort, ":")
		if host != "" {
			port = strconv.Itoa(defaultPort)
		}
	}

	if Verbose {
		log.Println("Connection:")
		log.Println("\tusername:\t", username)
		log.Println("\tpassword:\t", password)
		log.Println("\thas password:\t", hasPassword)
		log.Println("\thost:\t\t", host)
		log.Println("\tport:\t\t", port)
		log.Println("\tdbname:\t\t", dbname)
		log.Println()
	}

	return username, password, hasPassword, host, port, dbname
}

// Build an URL
func buildConnectionString(username, password string, hasPassword bool, host, port, dbname string) string {
	// Build a new connection string
	var buf bytes.Buffer

	buf.WriteString("postgres://")

	if (username != "") && hasPassword {
		buf.WriteString(username + ":" + password + "@")
	} else if username != "" {
		buf.WriteString(username + "@")
	} else if hasPassword {
		buf.WriteString(":" + password + "@")
	}

	if host != "" {
		buf.WriteString(host)
	}
	if port != "" {
		buf.WriteString(":" + port)
	}

	buf.WriteString("/?sslmode=disable")

	if Verbose {
		log.Println("Connection string:", buf.String())
	}

	return buf.String()
}

// Take apart and rebuild the connection string, as a sanity check. Also extract and return the dbname.
func examineConnectionString(connectionString string) (string, string) {
	username, password, hasPassword, hostname, port, dbname := splitConnectionString(connectionString)
	return buildConnectionString(username, password, hasPassword, hostname, port, dbname), dbname
}
