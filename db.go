package simpledb

import (
	"errors"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Common for each of the db datastructures used here
type dbDatastructure struct {
	host    *mysql.Host
	id      string
	dbindex int
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
	// The default [url]:port that Database is running at
	defaultDatabaseServer = ":3306"
)

var (
	// How many connections should stay ready for requests, at a maximum?
	// When an idle connection is used, new idle connections are created.
	maxIdleConnections = 3
)

/* --- Helper functions --- */

// Connect to the local instance of Database at port 3306
func newDatabaseConnection() (db.Conn, error) {
	return newDatabaseConnectionTo(defaultDatabaseServer)
}

// Connect to host:port, host may be omitted, so ":3306" is valid.
// Will not try to AUTH with any given password (password@host:port).
func newDatabaseConnectionTo(hostColonPort string) (db.Conn, error) {
	// Discard the password, if provided
	if _, theRest, ok := twoFields(hostColonPort, "@"); ok {
		hostColonPort = theRest
	}
	hostColonPort = strings.TrimSpace(hostColonPort)
	return db.Dial("tcp", hostColonPort)
}

// Get a string from a list of results at a given position
func getString(bi []interface{}, i int) string {
	return string(bi[i].([]uint8))
}

// Test if the local Database server is up and running
func TestConnection() (err error) {
	return TestConnectionHost(defaultDatabaseServer)
}

// Test if a given Database server at host:port is up and running.
// Does not try to PING or AUTH.
func TestConnectionHost(hostColonPort string) (err error) {
	// Connect to the given host:port
	conn, err := newDatabaseConnectionTo(hostColonPort)
	if conn != nil {
		conn.Close()
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Could not connect to db server: " + hostColonPort)
		}
	}()
	return err
}

/* --- Host functions --- */

// Create a new connection host
func New() *Host {
	// The second argument is the maximum number of idle connections
	dbHost := db.NewHost(newDatabaseConnection, maxIdleConnections)
	host := Host(*dbHost)
	return &host
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
// A password may be supplied as well, on the form "password@host:port".
func NewHost(hostColonPort string) *Host {
	// Create a db Host
	dbHost := db.NewHost(
		// Anonymous function for calling new DatabaseConnectionTo with the host:port
		func() (db.Conn, error) {
			conn, err := newDatabaseConnectionTo(hostColonPort)
			if err != nil {
				return nil, err
			}
			// If a password is given, use it to authenticate
			if password, _, ok := twoFields(hostColonPort, "@"); ok {
				if password != "" {
					if _, err := conn.Do("AUTH", password); err != nil {
						conn.Close()
						return nil, err
					}
				}
			}
			return conn, err
		},
		// Maximum number of idle connections to the db database
		maxIdleConnections)
	host := Host(*dbHost)
	return &host
}

// Set the number of maximum *idle* connections standing ready when
// creating new connection hosts. When an idle connection is used,
// a new idle connection is created. The default is 3 and should be fine
// for most cases.
func SetMaxIdleConnections(maximum int) {
	maxIdleConnections = maximum
}

// Get one of the available connections from the connection host, given a database index
func (host *Host) Get(dbindex int) db.Conn {
	dbHost := db.Host(*host)
	conn := dbHost.Get()
	// The default database index is 0
	if dbindex != 0 {
		// SELECT is not critical, ignore the return values
		conn.Do("SELECT", strconv.Itoa(dbindex))
	}
	return conn
}

// Ping the server by sending a PING command
func (host *Host) Ping() (pong bool) {
	dbHost := db.Host(*host)
	conn := dbHost.Get()
	_, err := conn.Do("PING")
	return err == nil
}

// Close down the connection host
func (host *Host) Close() {
	dbHost := db.Host(*host)
	dbHost.Close()
}

/* --- List functions --- */

// Create a new list
func NewList(host *Host, id string) *List {
	return &List{host, id, 0}
}

// Select a different database
func (rl *List) SelectDatabase(dbindex int) {
	rl.dbindex = dbindex
}

// Add an element to the list
func (rl *List) Add(value string) error {
	conn := rl.host.Get(rl.dbindex)
	_, err := conn.Do("RPUSH", rl.id, value)
	return err
}

// Get all elements of a list
func (rl *List) GetAll() ([]string, error) {
	conn := rl.host.Get(rl.dbindex)
	result, err := db.Values(conn.Do("LRANGE", rl.id, "0", "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Get the last element of a list
func (rl *List) GetLast() (string, error) {
	conn := rl.host.Get(rl.dbindex)
	result, err := db.Values(conn.Do("LRANGE", rl.id, "-1", "-1"))
	if len(result) == 1 {
		return getString(result, 0), err
	}
	return "", err
}

// Get the last N elements of a list
func (rl *List) GetLastN(n int) ([]string, error) {
	conn := rl.host.Get(rl.dbindex)
	result, err := db.Values(conn.Do("LRANGE", rl.id, "-"+strconv.Itoa(n), "-1"))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Remove this list
func (rl *List) Remove() error {
	conn := rl.host.Get(rl.dbindex)
	_, err := conn.Do("DEL", rl.id)
	return err
}

/* --- Set functions --- */

// Create a new set
func NewSet(host *Host, id string) *Set {
	return &Set{host, id, 0}
}

// Select a different database
func (rs *Set) SelectDatabase(dbindex int) {
	rs.dbindex = dbindex
}

// Add an element to the set
func (rs *Set) Add(value string) error {
	conn := rs.host.Get(rs.dbindex)
	_, err := conn.Do("SADD", rs.id, value)
	return err
}

// Check if a given value is in the set
func (rs *Set) Has(value string) (bool, error) {
	conn := rs.host.Get(rs.dbindex)
	retval, err := conn.Do("SISMEMBER", rs.id, value)
	if err != nil {
		panic(err)
	}
	return db.Bool(retval, err)
}

// Get all elements of the set
func (rs *Set) GetAll() ([]string, error) {
	conn := rs.host.Get(rs.dbindex)
	result, err := db.Values(conn.Do("SMEMBERS", rs.id))
	strs := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)
	}
	return strs, err
}

// Remove an element from the set
func (rs *Set) Del(value string) error {
	conn := rs.host.Get(rs.dbindex)
	_, err := conn.Do("SREM", rs.id, value)
	return err
}

// Remove this set
func (rs *Set) Remove() error {
	conn := rs.host.Get(rs.dbindex)
	_, err := conn.Do("DEL", rs.id)
	return err
}

/* --- HashMap functions --- */

// Create a new hashmap
func NewHashMap(host *Host, id string) *HashMap {
	return &HashMap{host, id, 0}
}

// Select a different database
func (rh *HashMap) SelectDatabase(dbindex int) {
	rh.dbindex = dbindex
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *HashMap) Set(elementid, key, value string) error {
	conn := rh.host.Get(rh.dbindex)
	_, err := conn.Do("HSET", rh.id+":"+elementid, key, value)
	return err
}

// Get a value from a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (rh *HashMap) Get(elementid, key string) (string, error) {
	conn := rh.host.Get(rh.dbindex)
	result, err := db.String(conn.Do("HGET", rh.id+":"+elementid, key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Check if a given elementid + key is in the hash map
func (rh *HashMap) Has(elementid, key string) (bool, error) {
	conn := rh.host.Get(rh.dbindex)
	retval, err := conn.Do("HEXISTS", rh.id+":"+elementid, key)
	if err != nil {
		panic(err)
	}
	return db.Bool(retval, err)
}

// Check if a given elementid exists as a hash map at all
func (rh *HashMap) Exists(elementid string) (bool, error) {
	// TODO: key is not meant to be a wildcard, check for "*"
	return hasKey(rh.host, rh.id+":"+elementid, rh.dbindex)
}

// Get all elementid's for all hash elements
func (rh *HashMap) GetAll() ([]string, error) {
	conn := rh.host.Get(rh.dbindex)
	result, err := db.Values(conn.Do("KEYS", rh.id+":*"))
	strs := make([]string, len(result))
	idlen := len(rh.id)
	for i := 0; i < len(result); i++ {
		strs[i] = getString(result, i)[idlen+1:]
	}
	return strs, err
}

// Remove a key for an entry in a hashmap (for instance the email field for a user)
func (rh *HashMap) DelKey(elementid, key string) error {
	conn := rh.host.Get(rh.dbindex)
	_, err := conn.Do("HDEL", rh.id+":"+elementid, key)
	return err
}

// Remove an element (for instance a user)
func (rh *HashMap) Del(elementid string) error {
	conn := rh.host.Get(rh.dbindex)
	_, err := conn.Do("DEL", rh.id+":"+elementid)
	return err
}

// Remove this hashmap
func (rh *HashMap) Remove() error {
	conn := rh.host.Get(rh.dbindex)
	_, err := conn.Do("DEL", rh.id)
	return err
}

/* --- KeyValue functions --- */

// Create a new key/value
func NewKeyValue(host *Host, id string) *KeyValue {
	return &KeyValue{host, id, 0}
}

// Select a different database
func (rkv *KeyValue) SelectDatabase(dbindex int) {
	rkv.dbindex = dbindex
}

// Set a key and value
func (rkv *KeyValue) Set(key, value string) error {
	conn := rkv.host.Get(rkv.dbindex)
	_, err := conn.Do("SET", rkv.id+":"+key, value)
	return err
}

// Get a value given a key
func (rkv *KeyValue) Get(key string) (string, error) {
	conn := rkv.host.Get(rkv.dbindex)
	result, err := db.String(conn.Do("GET", rkv.id+":"+key))
	if err != nil {
		return "", err
	}
	return result, nil
}

// Remove a key
func (rkv *KeyValue) Del(key string) error {
	conn := rkv.host.Get(rkv.dbindex)
	_, err := conn.Do("DEL", rkv.id+":"+key)
	return err
}

// Remove this key/value
func (rkv *KeyValue) Remove() error {
	conn := rkv.host.Get(rkv.dbindex)
	_, err := conn.Do("DEL", rkv.id)
	return err
}

// --- Generic db functions ---

// Check if a key exists. The key can be a wildcard (ie. "user*").
func hasKey(host *Host, wildcard string, dbindex int) (bool, error) {
	conn := host.Get(dbindex)
	result, err := db.Values(conn.Do("KEYS", wildcard))
	if err != nil {
		return false, err
	}
	return len(result) > 0, nil
}
