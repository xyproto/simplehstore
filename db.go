// Simple way to use a MySQL/MariaDB database
package db

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
	"strings"
)

type Host struct {
	db     *sql.DB
	dbname string
}

// Common for each of the db datastructures used here
type dbDatastructure struct {
	host  *Host
	table string
}

type (
	List     dbDatastructure
	Set      dbDatastructure
	HashMap  dbDatastructure
	KeyValue dbDatastructure
)

const (
	// Version number. Stable API within major version numbers.
	Version = 1.0

	// The default "username:password@host:port/database" that the database is running at
	defaultDatabaseServer = ""     // "username:password@server:port/"
	defaultDatabaseName   = "test" // "main"
	defaultStringLength   = 42     // using VARCHAR, so this will be expanded up to 65535 characters as needed, unless mysql strict mode is enabled
	defaultPort           = 3306

	listCol  = "a_list"
	setCol   = "a_set"
	keyCol   = "property"
	valCol   = "value"
	ownerCol = "owner"
	kvCol    = "a_kv"
)

// Test if the local database server is up and running.
func TestConnection() (err error) {
	return TestConnectionHost(defaultDatabaseServer)
}

// Test if a given database server is up and running.
// connectionString may be on the form "username:password@host:port/database".
func TestConnectionHost(connectionString string) (err error) {
	newConnectionString, _ := rebuildConnectionString(connectionString)
	// Connect to the given host:port
	db, err := sql.Open("mysql", newConnectionString)
	defer db.Close()
	err = db.Ping()
	if Verbose {
		if err != nil {
			log.Println("Ping: failed")
		} else {
			log.Println("Ping: ok")
		}
	}
	return err
}

/* --- Host functions --- */

// Create a new database connection.
// connectionString may be on the form "username:password@host:port/database".
func NewHost(connectionString string) *Host {

	newConnectionString, dbname := rebuildConnectionString(connectionString)

	db, err := sql.Open("mysql", newConnectionString)
	if err != nil {
		log.Fatalln("Could not connect to " + newConnectionString + "!")
	}
	host := &Host{db, dbname}
	if err := host.Ping(); err != nil {
		log.Fatalln("Host does not reply to ping: " + err.Error())
	}
	if err := host.createDatabase(); err != nil {
		log.Fatalln("Could not create database " + host.dbname + ": " + err.Error())
	}
	if err := host.useDatabase(); err != nil {
		panic("Could not use database " + host.dbname + ": " + err.Error())
	}
	return host
}

// The default database connection
func New() *Host {
	connectionString := defaultDatabaseServer + defaultDatabaseName
	if !strings.HasSuffix(defaultDatabaseServer, "/") {
		connectionString = defaultDatabaseServer + "/" + defaultDatabaseName
	}
	return NewHost(connectionString)
}

// Select a different database. Create the database if needed.
func (host *Host) SelectDatabase(dbname string) error {
	host.dbname = dbname
	if err := host.createDatabase(); err != nil {
		return err
	}
	if err := host.useDatabase(); err != nil {
		return err
	}
	return nil
}

// Will create the database if it does not already exist
func (host *Host) createDatabase() error {
	if _, err := host.db.Exec("CREATE DATABASE IF NOT EXISTS " + host.dbname + " CHARACTER SET = utf8"); err != nil {
		return err
	}
	if Verbose {
		log.Println("Created database " + host.dbname)
	}
	return nil
}

// Use the host.dbname database
func (host *Host) useDatabase() error {
	if _, err := host.db.Exec("USE " + host.dbname); err != nil {
		return err
	}
	if Verbose {
		log.Println("Using database " + host.dbname)
	}
	return nil
}

// Close the connection
func (host *Host) Close() {
	host.db.Close()
}

// Ping the host
func (host *Host) Ping() error {
	return host.db.Ping()
}

/* --- List functions --- */

// Create a new list. Lists are ordered.
func NewList(host *Host, name string) *List {
	l := &List{host, name}
	// list is the name of the column
	if _, err := l.host.db.Exec("CREATE TABLE IF NOT EXISTS " + name + " (id INT PRIMARY KEY AUTO_INCREMENT, " + listCol + " VARCHAR(" + strconv.Itoa(defaultStringLength) + "))"); err != nil {
		// This is more likely to happen at the start of the program,
		// hence the panic.
		panic("Could not create table " + name + ": " + err.Error())
	}
	if Verbose {
		log.Println("Created table " + name + " in database " + host.dbname)
	}
	return l
}

// Add an element to the list
func (rl *List) Add(value string) error {
	// list is the name of the column
	_, err := rl.host.db.Exec("INSERT INTO "+rl.table+" ("+listCol+") VALUES (?)", value)
	return err
}

// Get all elements of a list
func (rl *List) GetAll() ([]string, error) {
	rows, err := rl.host.db.Query("SELECT " + listCol + " FROM " + rl.table + " ORDER BY id")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var (
		values []string
		value  string
	)
	for rows.Next() {
		err = rows.Scan(&value)
		values = append(values, value)
		if err != nil {
			panic(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return values, nil
}

// Get the last element of a list
func (rl *List) GetLast() (string, error) {
	// Fetches the item with the largest id.
	// Faster than "ORDER BY id DESC limit 1" for large tables.
	rows, err := rl.host.db.Query("SELECT " + listCol + " FROM " + rl.table + " WHERE id = (SELECT MAX(id) FROM " + rl.table + ")")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value string
	// Get the value. Will only loop once.
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			panic(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return value, nil
}

// Get the last N elements of a list
func (rl *List) GetLastN(n int) ([]string, error) {
	rows, err := rl.host.db.Query("SELECT " + listCol + " FROM (SELECT * FROM " + rl.table + " ORDER BY id DESC limit " + strconv.Itoa(n) + ")sub ORDER BY id ASC")
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var (
		values []string
		value  string
	)
	for rows.Next() {
		err = rows.Scan(&value)
		values = append(values, value)
		if err != nil {
			panic(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	if len(values) < n {
		return []string{}, errors.New("Too few elements in table at GetLastN")
	}
	return values, nil
}

// Remove this list
func (rl *List) Remove() error {
	// Remove the table
	_, err := rl.host.db.Exec("DROP TABLE " + rl.table)
	return err
}

// Clear the list contents
func (rl *List) Clear() error {
	// Clear the table
	_, err := rl.host.db.Exec("TRUNCATE TABLE " + rl.table)
	return err
}

/* --- Set functions --- */

// Create a new set
func NewSet(host *Host, name string) *Set {
	s := &Set{host, name}
	// list is the name of the column
	if _, err := s.host.db.Exec("CREATE TABLE IF NOT EXISTS " + name + " (" + setCol + " VARCHAR(" + strconv.Itoa(defaultStringLength) + "))"); err != nil {
		// This is more likely to happen at the start of the program, hence the panic.
		panic("Could not create table " + name + ": " + err.Error())
	}
	if Verbose {
		log.Println("Created table " + name + " in database " + host.dbname)
	}
	return s
}

// Add an element to the set
func (s *Set) Add(value string) error {
	// Check if the value is not already there before adding
	has, err := s.Has(value)
	if !has && (err == nil) {
		_, err = s.host.db.Exec("INSERT INTO "+s.table+" ("+setCol+") VALUES (?)", value)
	}
	return err
}

// Check if a given value is in the set
func (s *Set) Has(value string) (bool, error) {
	rows, err := s.host.db.Query("SELECT "+setCol+" FROM "+s.table+" WHERE "+setCol+" = ?", value)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var scanValue string
	// Get the value. Should not loop more than once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&scanValue)
		if err != nil {
			panic(err.Error())
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	if counter > 1 {
		panic("Duplicate members in set! " + value)
	}
	return counter > 0, nil
}

// Get all elements of the set
func (s *Set) GetAll() ([]string, error) {
	rows, err := s.host.db.Query("SELECT " + setCol + " FROM " + s.table)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var (
		values []string
		value  string
	)
	for rows.Next() {
		err = rows.Scan(&value)
		values = append(values, value)
		if err != nil {
			panic(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return values, nil
}

// Remove an element from the set
func (s *Set) Del(value string) error {
	// Remove a value from the table
	_, err := s.host.db.Exec("DELETE FROM " + s.table + " WHERE " + setCol + " = " + value)
	return err
}

// Remove this set
func (s *Set) Remove() error {
	// Remove the table
	_, err := s.host.db.Exec("DROP TABLE " + s.table)
	return err
}

// Clear the list contents
func (s *Set) Clear() error {
	// Clear the table
	_, err := s.host.db.Exec("TRUNCATE TABLE " + s.table)
	return err
}

/* --- HashMap functions --- */

// Create a new hashmap
func NewHashMap(host *Host, name string) *HashMap {
	h := &HashMap{host, name}
	sqltype := "VARCHAR(" + strconv.Itoa(defaultStringLength) + ")"
	// Using three columns: element id, key and value
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s, %s %s, %s %s)", name, ownerCol, sqltype, keyCol, sqltype, valCol, sqltype)
	if _, err := h.host.db.Exec(query); err != nil {
		// This is more likely to happen at the start of the program,
		// hence the panic.
		panic("Could not create table " + name + ": " + err.Error())
	}
	if Verbose {
		log.Println("Created table " + name + " in database " + host.dbname)
	}
	return h
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (h *HashMap) Set(owner, key, value string) error {
	// See if the owner and key already exists
	ok, err := h.Has(owner, key)
	if err != nil {
		return err
	}
	if Verbose {
		log.Printf("%s/%s exists? %v\n", owner, key, ok)
	}
	if ok {
		_, err = h.host.db.Exec("UPDATE "+h.table+" SET "+valCol+" = ? WHERE "+ownerCol+" = ? AND "+keyCol+" = ?", value, owner, key)
		if Verbose {
			log.Println("Updated the table: " + h.table)
		}
	} else {
		_, err = h.host.db.Exec("INSERT INTO "+h.table+" ("+ownerCol+", "+keyCol+", "+valCol+") VALUES (?, ?, ?)", owner, key, value)
		if Verbose {
			log.Println("Added to the table: " + h.table)
		}
	}
	return err
}

// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password").
func (h *HashMap) Get(owner, key string) (string, error) {
	rows, err := h.host.db.Query("SELECT "+valCol+" FROM "+h.table+" WHERE "+ownerCol+" = ? AND "+keyCol+" = ?", owner, key)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value string
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			panic(err.Error())
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	if counter == 0 {
		return "", errors.New("No such owner/key: " + owner + "/" + key)
	}
	return value, nil
}

// Check if a given owner + key is in the hash map
func (h *HashMap) Has(owner, key string) (bool, error) {
	rows, err := h.host.db.Query("SELECT "+valCol+" FROM "+h.table+" WHERE "+ownerCol+" = ? AND "+keyCol+" = ?", owner, key)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value string
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			panic(err.Error())
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	if counter > 1 {
		panic("Duplicate keys in hash map! " + value)
	}
	return counter > 0, nil
}

// Check if a given owner exists as a hash map at all
func (h *HashMap) Exists(owner string) (bool, error) {
	rows, err := h.host.db.Query("SELECT "+valCol+" FROM "+h.table+" WHERE "+ownerCol+" = ?", owner)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value string
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			panic(err.Error())
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return counter > 0, nil
}

// Get all owner's for all hash elements
func (h *HashMap) GetAll() ([]string, error) {
	rows, err := h.host.db.Query("SELECT " + ownerCol + " FROM " + h.table)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var (
		values []string
		value  string
	)
	for rows.Next() {
		err = rows.Scan(&value)
		values = append(values, value)
		if err != nil {
			panic(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return values, nil
}

// Remove a key for an entry in a hashmap (for instance the email field for a user)
func (h *HashMap) DelKey(owner, key string) error {
	// Remove a key from the hashmap
	_, err := h.host.db.Exec("DELETE FROM "+h.table+" WHERE "+ownerCol+" = ? AND "+keyCol+" = ?", owner, key)
	return err
}

// Remove an element (for instance a user)
func (h *HashMap) Del(owner string) error {
	// Remove an element id from the table
	_, err := h.host.db.Exec("DELETE FROM "+h.table+" WHERE "+ownerCol+" = ?", owner)
	return err
}

// Remove this hashmap
func (h *HashMap) Remove() error {
	// Remove the table
	_, err := h.host.db.Exec("DROP TABLE " + h.table)
	return err
}

func (h *HashMap) Clear() error {
	// Clear the table
	_, err := h.host.db.Exec("TRUNCATE TABLE " + h.table)
	return err
}

/* --- KeyValue functions --- */

// Create a new key/value
func NewKeyValue(host *Host, name string) *KeyValue {
	kv := &KeyValue{host, name}
	sqltype := "VARCHAR(" + strconv.Itoa(defaultStringLength) + ")"
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s, %s %s)", name, keyCol, sqltype, valCol, sqltype)
	if _, err := kv.host.db.Exec(query); err != nil {
		// This is more likely to happen at the start of the program,
		// hence the panic.
		panic("Could not create table " + name + ": " + err.Error())
	}
	if Verbose {
		log.Println("Created table " + name + " in database " + host.dbname)
	}
	return kv

}

// Set a key and value
func (kv *KeyValue) Set(key, value string) error {
	if _, err := kv.Get(key); err != nil {
		// Key does not exist, create it
		_, err = kv.host.db.Exec("INSERT INTO "+kv.table+" ("+keyCol+", "+valCol+") VALUES (?, ?)", key, value)
		return err
	} else {
		// Key exists, update the value
		_, err := kv.host.db.Exec("UPDATE "+kv.table+" SET "+valCol+" = ? WHERE "+keyCol+" = ?", value, key)
		return err
	}
}

// Get a value given a key
func (kv *KeyValue) Get(key string) (string, error) {
	rows, err := kv.host.db.Query("SELECT "+valCol+" FROM "+kv.table+" WHERE "+keyCol+" = ?", key)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var value string
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			panic(err.Error())
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	if counter != 1 {
		return "", errors.New("Wrong number of keys in KeyValue table: " + kv.table)
	}
	return value, nil
}

// Remove a key
func (kv *KeyValue) Del(key string) error {
	_, err := kv.host.db.Exec("DELETE FROM "+kv.table+" WHERE "+keyCol+" = ?", key)
	return err
}

// Remove this key/value
func (kv *KeyValue) Remove() error {
	// Remove the table
	_, err := kv.host.db.Exec("DROP TABLE " + kv.table)
	return err
}

// Clear this key/value
func (kv *KeyValue) Clear() error {
	// Remove the table
	_, err := kv.host.db.Exec("TRUNCATE TABLE " + kv.table)
	return err
}
