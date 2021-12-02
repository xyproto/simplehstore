package simplehstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// KeyValue is a hash map with a key and a value, stored in PostgreSQL
type KeyValue dbDatastructure

// NewKeyValue creates a new KeyValue struct, for storing key/value pairs.
func NewKeyValue(host *Host, name string) (*KeyValue, error) {
	kv := &KeyValue{host, name}

	// Create extension hstore
	query := "CREATE EXTENSION hstore"
	// Ignore erors if this is already created
	kv.host.db.Exec(query)

	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (attr hstore)", pq.QuoteIdentifier(kvPrefix+kv.table))
	if _, err := kv.host.db.Exec(query); err != nil {
		return nil, err
	}
	if Verbose {
		log.Println("Created HSTORE table " + pq.QuoteIdentifier(kvPrefix+kv.table) + " in database " + host.dbname)
	}
	return kv, nil
}

// CreateIndexTable creates an INDEX table for this key/value, that may speed up lookups
func (kv *KeyValue) CreateIndexTable() error {
	// strip double quotes from kv.table and add _idx at the end
	indexTableName := strings.TrimSuffix(strings.TrimPrefix(kv.table, "\""), "\"") + "_idx"
	query := fmt.Sprintf("CREATE INDEX %q ON %s USING GIN (attr)", indexTableName, kv.table)
	if Verbose {
		fmt.Println(query)
	}
	_, err := kv.host.db.Exec(query)
	return err
}

// RemoveIndexTable removes the INDEX table for this key/value
func (kv *KeyValue) RemoveIndexTable() error {
	// strip double quotes from kv.table and add _idx at the end
	indexTableName := strings.TrimSuffix(strings.TrimPrefix(kv.table, "\""), "\"") + "_idx"
	query := fmt.Sprintf("DROP INDEX %q", indexTableName)
	if Verbose {
		fmt.Println(query)
	}
	_, err := kv.host.db.Exec(query)
	return err
}

// insert a new key+value in the current KeyValue table
func (kv *KeyValue) insert(key, encodedValue string) (int64, error) {
	// Try inserting
	query := fmt.Sprintf("INSERT INTO %s (attr) VALUES ('\"%s\"=>\"%s\"')", pq.QuoteIdentifier(kvPrefix+kv.table), escapeSingleQuotes(key), escapeSingleQuotes(encodedValue))
	if Verbose {
		fmt.Println(query)
	}
	result, err := kv.host.db.Exec(query)
	if Verbose {
		log.Println("keyValue insert: inserted row into: "+kv.table+" err? ", err)
	}
	n, _ := result.RowsAffected()
	return n, err
}

// insert a new key+value in the current KeyValue table, as part of a transaction
func (kv *KeyValue) insertWithTransaction(ctx context.Context, transaction *sql.Tx, key, encodedValue string) (int64, error) {
	// Try inserting
	query := fmt.Sprintf("INSERT INTO %s (attr) VALUES ('\"%s\"=>\"%s\"')", pq.QuoteIdentifier(kvPrefix+kv.table), escapeSingleQuotes(key), escapeSingleQuotes(encodedValue))
	if Verbose {
		fmt.Println(query)
	}
	result, err := transaction.ExecContext(ctx, query)
	if Verbose {
		log.Println("keyValue insertWithTransaction: inserted row into: "+kv.table+" err? ", err)
	}
	n, _ := result.RowsAffected()
	return n, err
}

// update a value in the current KeyValue table
func (kv *KeyValue) update(key, encodedValue string) (int64, error) {
	// Try updating
	query := fmt.Sprintf("UPDATE %s SET attr = attr || '\"%s\"=>\"%s\"' :: hstore", pq.QuoteIdentifier(kvPrefix+kv.table), escapeSingleQuotes(key), escapeSingleQuotes(encodedValue))
	if Verbose {
		fmt.Println(query)
	}
	result, err := kv.host.db.Exec(query)
	if Verbose {
		log.Println("Updated row in: "+kv.table+" err? ", err)
	}
	if result == nil {
		return 0, fmt.Errorf("keyValue update: no result when trying to update %s with a value", key)
	}
	n, _ := result.RowsAffected()
	return n, err
}

// update a value in the current KeyValue table, as part of a transaction
func (kv *KeyValue) updateWithTransaction(ctx context.Context, transaction *sql.Tx, key, encodedValue string) (int64, error) {
	// Try updating
	query := fmt.Sprintf("UPDATE %s SET attr = attr || '\"%s\"=>\"%s\"' :: hstore", pq.QuoteIdentifier(kvPrefix+kv.table), escapeSingleQuotes(key), escapeSingleQuotes(encodedValue))
	if Verbose {
		fmt.Println(query)
	}
	result, err := transaction.ExecContext(ctx, query)
	if Verbose {
		log.Println("Updated row in: "+kv.table+" err? ", err)
	}
	if result == nil {
		return 0, fmt.Errorf("keyValue updateWithTransaction: no result when trying to update %s with a value", key)
	}
	n, _ := result.RowsAffected()
	return n, err
}

// Set a key and value
func (kv *KeyValue) Set(key, value string) error {
	if !kv.host.rawUTF8 {
		Encode(&value)
	}
	encodedValue := value
	// First try updating the key/values
	n, err := kv.update(key, encodedValue)
	if err != nil {
		return err
	}
	// If no rows are affected (SELECTED) by the update, try inserting a row instead
	if n == 0 {
		n, err = kv.insert(key, encodedValue)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("keyValue Set: could not update or insert any rows")
		}
	}
	// success
	return nil
}

// set a key and value, as part of a transaction
func (kv *KeyValue) setWithTransaction(ctx context.Context, transaction *sql.Tx, key, encodedValue string) error {
	// First try updating the key/values
	n, err := kv.updateWithTransaction(ctx, transaction, key, encodedValue)
	if err != nil {
		return err
	}
	// If no rows are affected (SELECTED) by the update, try inserting a row instead
	if n == 0 {
		n, err = kv.insertWithTransaction(ctx, transaction, key, encodedValue)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("could not update or insert any rows")
		}
	}
	// success
	return nil
}

// SetCheck will set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
// Returns true if the key already existed.
func (kv *KeyValue) SetCheck(key, value string) (bool, error) {
	if !kv.host.rawUTF8 {
		Encode(&value)
	}
	encodedValue := value
	// First try updating the key/values
	n, err := kv.update(key, encodedValue)
	if err != nil {
		return false, err
	}
	// If no rows are affected (SELECTED) by the update, try inserting a row instead
	if n == 0 {
		n, err = kv.insert(key, encodedValue)
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, errors.New("could not update or insert any rows")
		}
		return false, nil
	}
	// success, and the key already existed
	return true, nil
}

// Get a value given a key
func (kv *KeyValue) Get(key string) (string, error) {
	rows, err := kv.host.db.Query(fmt.Sprintf("SELECT attr -> '%s' FROM %s", escapeSingleQuotes(key), pq.QuoteIdentifier(kvPrefix+kv.table)))
	if err != nil {
		return "", fmt.Errorf("KeyValue.Get: query error: %s", err)
	}
	if rows == nil {
		return "", fmt.Errorf("KeyValue.Get: no rows for key %s", key)
	}
	defer rows.Close()
	var value sql.NullString
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return "", err
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("keyValue Get: rows.Err(): %s", err)
	}
	if counter == 0 {
		return "", errors.New("keyValue Get: no rows")
	}
	if counter != 1 {
		return "", fmt.Errorf("keyValue Get: wrong number of keys in KeyValue table: %s", kvPrefix+kv.table)
	}

	s := value.String
	if !kv.host.rawUTF8 {
		Decode(&s)
	}
	if s == "" {
		return "", fmt.Errorf("key does not exist: %s", key)
	}
	return s, nil
}

// Get a value given a key
func (kv *KeyValue) getWithTransaction(ctx context.Context, transaction *sql.Tx, key string) (string, error) {
	rows, err := transaction.QueryContext(ctx, fmt.Sprintf("SELECT attr -> '%s' FROM %s", escapeSingleQuotes(key), pq.QuoteIdentifier(kvPrefix+kv.table)))
	if err != nil {
		return "", fmt.Errorf("KeyValue getWithTransaction: query error: %s", err)
	}
	if rows == nil {
		return "", fmt.Errorf("KeyValue getWithTransaction: no rows for key %s", key)
	}
	defer rows.Close()
	var value sql.NullString
	// Get the value. Should only loop once.
	counter := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return "", err
		}
		counter++
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("keyValue getWithTransaction: rows.Err(): %s", err)
	}

	if counter == 0 {
		return "", errors.New("keyValue getWithTransaction: no rows")
	}

	if counter != 1 {
		return "", fmt.Errorf("keyValue getWithTransaction: wrong number of keys in KeyValue table: %s", kvPrefix+kv.table)
	}
	s := value.String
	if !kv.host.rawUTF8 {
		Decode(&s)
	}
	if s == "" {
		return "", fmt.Errorf("key does not exist: %s", key)
	}
	return s, nil
}

// Inc increases the value of a key and returns the new value.
// Returns "1" if no previous value is found.
func (kv *KeyValue) Inc(key string) (string, error) {
	// Retrieve the current value, if any
	num := 0
	// See if we can fetch an existing value.
	if val, err := kv.Get(key); err == nil { // success
		// See if we can convert the value to a number.
		if converted, errConv := strconv.Atoi(val); errConv == nil { // success
			num = converted
		}
	} else {
		// The key does not exist, create a new one.
		// This is to reflect the behavior of INCR in Redis.
		NewKeyValue(kv.host, kv.table)
	}
	// Num is now either 0 or the previous numeric value
	num++
	// Convert the new value to a string
	val := strconv.Itoa(num)
	// Store the new number
	if err := kv.Set(key, val); err != nil {
		// Saving the value failed
		return "0", err
	}
	// Success
	return val, nil
}

// Dec increases the value of a key and returns the new value.
// Returns "1" if no previous value is found.
func (kv *KeyValue) Dec(key string) (string, error) {
	// Retrieve the current value, if any
	num := 0
	// See if we can fetch an existing value. NOTE: "== nil"
	if val, err := kv.Get(key); err == nil {
		// See if we can convert the value to a number. NOTE: "== nil"
		if converted, errConv := strconv.Atoi(val); errConv == nil {
			num = converted
		}
	} else {
		// The key does not exist, create a new one.
		NewKeyValue(kv.host, kv.table)
	}
	// Num is now either 0 or the previous numeric value
	num--
	// Convert the new value to a string
	val := strconv.Itoa(num)
	// Store the new number
	if err := kv.Set(key, val); err != nil {
		// Saving the value failed
		return "0", err
	}
	// Success
	return val, nil
}

// Del removes the given key
func (kv *KeyValue) Del(key string) error {
	_, err := kv.host.db.Exec(fmt.Sprintf("UPDATE %s SET attr = delete(attr, '%s')", pq.QuoteIdentifier(kvPrefix+kv.table), escapeSingleQuotes(key)))
	return err
}

// Remove this key/value
func (kv *KeyValue) Remove() error {
	// Remove the table
	_, err := kv.host.db.Exec(fmt.Sprintf("DROP TABLE %s", pq.QuoteIdentifier(kvPrefix+kv.table)))
	return err
}

// Clear this key/value
func (kv *KeyValue) Clear() error {
	// Truncate the table
	_, err := kv.host.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", pq.QuoteIdentifier(kvPrefix+kv.table)))
	return err
}
