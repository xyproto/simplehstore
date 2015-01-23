package db

import (
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

// Common for each of the db datastructures used here
type dbDatastructure struct {
	host    *sql.DB
	id      string
	dbname  string
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
	// The default host:port that the database is running at
	defaultDatabaseServer = "go:go@/"
	defaultDatabaseName = "main"
	defaultStringLength = 255
)

/* --- Helper functions --- */

// Get a string from a list of results at a given position
func getString(bi []interface{}, i int) string {
	return string(bi[i].([]uint8))
}

// Test if the local database server is up and running
func TestConnection() (err error) {
	return TestConnectionHost(defaultDatabaseServer)
}

// Test if a given database server at host:port is up and running.
// Does not try to PING or AUTH.
func TestConnectionHost(hostColonPort string) (err error) {
	// Connect to the given host:port
	db, err := sql.Open("mysql", hostColonPort)
	defer db.Close()
	return db.Ping()
}

/* --- Host functions --- */

func New() *sql.DB {
	return NewHost(defaultDatabaseServer)
}

// Split a string into two parts, given a delimiter.
// Returns the two parts and true if it works out.
func twoFields(s, delim string) (string, string, bool) {
	if strings.Count(s, delim) != 1 {
		return s, "", false
	}
	fields := strings.Split(s, delim)
	return fields[0], fields[1], true
}

// Create a new connection host given a host:port string.
// Other options may be supplied on the form "username:password@host:port/database".
func NewHost(hostColonPort string) *sql.DB {
	db, err := sql.Open("mysql", hostColonPort)
	if err != nil {
		panic("Could not connect to " + defaultDatabaseServer + "!")
	}
	return db
}

/* --- List functions --- */

// Create a new list
func NewList(host *sql.DB, id string) *List {
	l := &List{host, id, defaultDatabaseName}
	l.SelectDatabase(l.dbname)
	if _, err := l.host.Exec("CREATE TABLE IF NOT EXISTS " + id + " (value CHAR)"); err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }
	return l
}

// Select a different database
func (rl *List) SelectDatabase(dbname string) {
	if _, err := rl.host.Exec("CREATE DATABASE IF NOT EXISTS " + dbname + " CHARACTER SET = utf8"); err != nil {
		panic(err.Error())
	}
	if _, err := rl.host.Exec("USE " + dbname); err != nil {
		panic(err.Error())
	}
	rl.dbname = dbname
}

// Add an element to the list
func (rl *List) Add(value string) error {
	_, err := rl.host.Exec("INSERT INTO " + rl.id + " VALUES ('" + value + "')")
	return err
}

// Get all elements of a list
func (rl *List) GetAll() (values []string, err error) {
	rows, err := rl.host.Query("SELECT * FROM " + rl.id)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		values = append(values, string(value))
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
	//result, err := db.Values(db.Do("LRANGE", rl.id, "-1", "-1"))
	//if len(result) == 1 {
	//	return getString(result, 0), err
	//}
	//return "", err
	return "", nil
}

// Get the last N elements of a list
func (rl *List) GetLastN(n int) ([]string, error) {
	//result, err := db.Values(db.Do("LRANGE", rl.id, "-"+strconv.Itoa(n), "-1"))
	//strs := make([]string, len(result))
	//for i := 0; i < len(result); i++ {
	//	strs[i] = getString(result, i)
	//}
	//return strs, err
	return []string{}, nil
}

// Remove this list
func (rl *List) Remove() error {
	//_, err := db.Do("DEL", rl.id)
	//return err
	return nil
}

/* --- Set functions --- */

//// Create a new set
//func NewSet(host *sql.DB, id string) *Set {
//	return &Set{host, id, defaultDatabaseName}
//}
//
//// Select a different database
//func (rs *Set) SelectDatabase(dbname string) {
//	rs.dbname = dbname
//}
//
//// Add an element to the set
//func (rs *Set) Add(value string) error {
//	db := rs.host.Get(rs.dbname)
//	_, err := db.Do("SADD", rs.id, value)
//	return err
//}
//
//// Check if a given value is in the set
//func (rs *Set) Has(value string) (bool, error) {
//	db := rs.host.Get(rs.dbname)
//	retval, err := db.Do("SISMEMBER", rs.id, value)
//	if err != nil {
//		panic(err)
//	}
//	return db.Bool(retval, err)
//}
//
//// Get all elements of the set
//func (rs *Set) GetAll() ([]string, error) {
//	db := rs.host.Get(rs.dbname)
//	result, err := db.Values(db.Do("SMEMBERS", rs.id))
//	strs := make([]string, len(result))
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)
//	}
//	return strs, err
//}
//
//// Remove an element from the set
//func (rs *Set) Del(value string) error {
//	db := rs.host.Get(rs.dbname)
//	_, err := db.Do("SREM", rs.id, value)
//	return err
//}
//
//// Remove this set
//func (rs *Set) Remove() error {
//	db := rs.host.Get(rs.dbname)
//	_, err := db.Do("DEL", rs.id)
//	return err
//}
//
///* --- HashMap functions --- */
//
//// Create a new hashmap
//func NewHashMap(host *sql.DB, id string) *HashMap {
//	return &HashMap{host, id, defaultDatabaseName}
//}
//
//// Select a different database
//func (rh *HashMap) SelectDatabase(dbname string) {
//	rh.dbname = dbname
//}
//
//// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
//func (rh *HashMap) Set(elementid, key, value string) error {
//	db := rh.host.Get(rh.dbname)
//	_, err := db.Do("HSET", rh.id+":"+elementid, key, value)
//	return err
//}
//
//// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
//func (rh *HashMap) Get(elementid, key string) (string, error) {
//	db := rh.host.Get(rh.dbname)
//	result, err := db.String(db.Do("HGET", rh.id+":"+elementid, key))
//	if err != nil {
//		return "", err
//	}
//	return result, nil
//}
//
//// Check if a given elementid + key is in the hash map
//func (rh *HashMap) Has(elementid, key string) (bool, error) {
//	db := rh.host.Get(rh.dbname)
//	retval, err := db.Do("HEXISTS", rh.id+":"+elementid, key)
//	if err != nil {
//		panic(err)
//	}
//	return db.Bool(retval, err)
//}
//
//// Check if a given elementid exists as a hash map at all
//func (rh *HashMap) Exists(elementid string) (bool, error) {
//	// TODO: key is not meant to be a wildcard, check for "*"
//	return hasKey(rh.host, rh.id+":"+elementid, rh.dbname)
//}
//
//// Get all elementid's for all hash elements
//func (rh *HashMap) GetAll() ([]string, error) {
//	db := rh.host.Get(rh.dbname)
//	result, err := db.Values(db.Do("KEYS", rh.id+":*"))
//	strs := make([]string, len(result))
//	idlen := len(rh.id)
//	for i := 0; i < len(result); i++ {
//		strs[i] = getString(result, i)[idlen+1:]
//	}
//	return strs, err
//}
//
//// Remove a key for an entry in a hashmap (for instance the email field for a user)
//func (rh *HashMap) DelKey(elementid, key string) error {
//	db := rh.host.Get(rh.dbname)
//	_, err := db.Do("HDEL", rh.id+":"+elementid, key)
//	return err
//}
//
//// Remove an element (for instance a user)
//func (rh *HashMap) Del(elementid string) error {
//	db := rh.host.Get(rh.dbname)
//	_, err := db.Do("DEL", rh.id+":"+elementid)
//	return err
//}
//
//// Remove this hashmap
//func (rh *HashMap) Remove() error {
//	db := rh.host.Get(rh.dbname)
//	_, err := db.Do("DEL", rh.id)
//	return err
//}
//
///* --- KeyValue functions --- */
//
//// Create a new key/value
//func NewKeyValue(host *sql.DB, id string) *KeyValue {
//	return &KeyValue{host, id, defaultDatabaseName}
//}
//
//// Select a different database
//func (rkv *KeyValue) SelectDatabase(dbname string) {
//	rkv.dbname = dbname
//}
//
//// Set a key and value
//func (rkv *KeyValue) Set(key, value string) error {
//	db := rkv.host.Get(rkv.dbname)
//	_, err := db.Do("SET", rkv.id+":"+key, value)
//	return err
//}
//
//// Get a value given a key
//func (rkv *KeyValue) Get(key string) (string, error) {
//	db := rkv.host.Get(rkv.dbname)
//	result, err := db.String(db.Do("GET", rkv.id+":"+key))
//	if err != nil {
//		return "", err
//	}
//	return result, nil
//}
//
//// Remove a key
//func (rkv *KeyValue) Del(key string) error {
//	db := rkv.host.Get(rkv.dbname)
//	_, err := db.Do("DEL", rkv.id+":"+key)
//	return err
//}
//
//// Remove this key/value
//func (rkv *KeyValue) Remove() error {
//	db := rkv.host.Get(rkv.dbname)
//	_, err := db.Do("DEL", rkv.id)
//	return err
//}
//
//// --- Generic db functions ---
//
//// Check if a key exists. The key can be a wildcard (ie. "user*").
//func hasKey(host *sql.DB, wildcard string, dbname string) (bool, error) {
//	db := host.Get(dbname)
//	result, err := db.Values(db.Do("KEYS", wildcard))
//	if err != nil {
//		return false, err
//	}
//	return len(result) > 0, nil
//}
