package simplehstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/lib/pq"
)

// JMap is a hash map backed by a table with JSONB data.
type JMap struct {
	host  *Host
	table string
}

// NewJMap creates a new JMap struct
func NewJMap(host *Host, name string) (*JMap, error) {
	jm := &JMap{host, name}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s NOT NULL, props JSONB)", pq.QuoteIdentifier(jmPrefix+jm.table), ownerCol, defaultStringType)

	if _, err := jm.host.db.Exec(query); err != nil {
		return nil, err
	}

	if Verbose {
		log.Println("Created JSONB table " + pq.QuoteIdentifier(jmPrefix+jm.table) + " in database " + host.dbname)
	}

	return jm, nil
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (jm *JMap) Set(owner, key, value string) error {
	query := fmt.Sprintf("UPDATE %s SET %s = jsonb_set(%s, '{%s}', '%s')", pq.QuoteIdentifier(jmPrefix+jm.table), ownerCol, ownerCol, key, value)
	if Verbose {
		fmt.Println(query)
	}
	_, err := jm.host.db.Exec(query)
	return err
}

// setPropWithTransaction will set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (jm *JMap) setPropWithTransaction(ctx context.Context, transaction *sql.Tx, owner, key, value string) error {
	query := fmt.Sprintf("UPDATE %s SET %s = jsonb_set(%s, '{%s}', '%s')", pq.QuoteIdentifier(jmPrefix+jm.table), ownerCol, ownerCol, key, value)
	if Verbose {
		fmt.Println(query)
	}
	result, err := transaction.ExecContext(ctx, query)
	if result == nil {
		return fmt.Errorf("jm setPropWithTransaction: no result when trying to update %s", key)
	}
	//return result.RowsAffected()
	return err
}

// SetMap will set many keys/values, in a single transaction
func (jm *JMap) SetMap(owner string, m map[string]string) error {
	// Use a context and a transaction to bundle queries
	ctx := context.Background()
	transaction, err := jm.host.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for k, v := range m {
		if err := jm.setPropWithTransaction(ctx, transaction, owner, k, v); err != nil {
			transaction.Rollback()
			return err
		}
	}
	return transaction.Commit()
}

// SetLargeMap will add many owners+keys/values, in a single transaction, without checking if they already exists.
// It also does not check if the keys or property keys contains fieldSep (¤) or not, for performance.
// These must all be brand new "usernames" (the first key), and not be in the existing hm2.OwnerSet().
// This function has good performance, but must be used carefully.
func (jm *JMap) SetLargeMap(allProperties map[string]map[string]string) error {
	ctx := context.Background()
	// Create a new transaction
	transaction, err := jm.host.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for owner, propMap := range allProperties {
		for k, v := range propMap {
			if err := jm.setPropWithTransaction(ctx, transaction, owner, k, v); err != nil {
				transaction.Rollback()
				return err
			}
		}
	}
	return transaction.Commit()
}

// Get a value.
// Returns: value, error
// If a value was not found, an empty string is returned.
func (jm *JMap) Get(owner, key string) (string, error) {
	return "", errors.New("not implemented")

	// 	if err != nil {
	// 		if noResult(err) {
	// 			return "", nil
	// 		}
	// 		return "", err
	// 	}
	// 	// No error and no value
	// 	if s == "" {
	// 		return s, nil
	// 	}
	// 	// No error and actually got a value
	// 	return s, nil

}

// Get multiple values
func (jm *JMap) GetMap(owner string, keys []string) (map[string]string, error) {
	return nil, errors.New("not implemented")

	// 	results := make(map[string]string)
	// 	// Use a context and a transaction to bundle queries
	// 	ctx := context.Background()
	// 	transaction, err := hm2.host.db.BeginTx(ctx, nil)
	// 	if err != nil {
	// 		return results, err
	// 	}
	// 	for _, key := range keys {
	// 		s, err := hm2.KeyValue().getWithTransaction(ctx, transaction, owner+fieldSep+key)
	// 		if err != nil {
	// 			transaction.Rollback()
	// 			return results, err
	// 		}
	// 		results[key] = s
	// 	}
	// 	transaction.Commit()
	// 	return results, nil

}

// Has checks if a given owner + key exists in the hash map
func (jm *JMap) Has(owner, key string) (bool, error) {
	return false, errors.New("not implemented")

	// 	s, err := hm2.KeyValue().Get(owner + fieldSep + key)
	// 	if err != nil {
	// 		if noResult(err) {
	// 			// Not an actual error, just got no results
	// 			return false, nil
	// 		}
	// 		// An actual error
	// 		return false, err
	// 	}
	// 	// No error, got a result
	// 	if s == "" {
	// 		return false, nil
	// 	}
	// 	return true, nil

}

// Exists checks if a given owner exists as a hash map at all
func (jm *JMap) Exists(owner string) (bool, error) {
	return false, errors.New("not implemented")

	// 	found, err := hm2.OwnerSet().Has(owner)
	// 	if err != nil {
	// 		// Either an actual error or no result
	// 		if noResult(err) {
	// 			return false, nil
	// 		}
	// 		// An actual error
	// 		return false, err
	// 	}
	// 	// Got a result, no error
	// 	return found, nil

}

// AllWhere returns all owner ID's that has a property where key == value
func (jm *JMap) AllWhere(key, value string) ([]string, error) {
	return []string{}, errors.New("not implemented")

	// 	allOwners, err := hm2.OwnerSet().All()
	// 	if err != nil {
	// 		return []string{}, err
	// 	}
	// 	// TODO: Improve the performance of this by using SQL instead of looping
	// 	foundOwners := []string{}
	// 	for _, owner := range allOwners {
	// 		// The owner+key exists and the value matches the given value
	// 		if v, err := hm2.Get(owner, key); err == nil && v == value {
	// 			foundOwners = append(foundOwners, owner)
	// 		}
	// 	}
	// 	return foundOwners, nil

}

// Keys loops through absolutely all owners and all properties in the database
// and returns all found keys.
func (jm *JMap) Keys(owner string) ([]string, error) {
	return []string{}, errors.New("not implemented")

	// 	allProps, err := hm2.PropSet().All()
	// 	if err != nil {
	// 		return []string{}, err
	// 	}
	// 	// TODO: Improve the performance of this by using SQL instead of looping
	// 	allKeys := []string{}
	// 	for _, key := range allProps {
	// 		fmt.Printf("HAS %s GOT %s? ", owner, key)
	// 		if found, err := hm2.Has(owner, key); err == nil && found {
	// 			fmt.Printf("YES\n")
	// 			allKeys = append(allKeys, key)
	// 		} else {
	// 			fmt.Printf("NO\n")
	// 		}
	// 	}
	// 	return allKeys, nil

}

// All returns all owner ID's
func (jm *JMap) All() ([]string, error) {
	return []string{}, errors.New("not implemented")
	//return hm2.OwnerSet().All()
}

// Count counts the number of owners for hash map elements
func (jm *JMap) Count() (int, error) {
	return 0, errors.New("not implemented")
	//return hm2.OwnerSet().Count()
}

// CountInt64 counts the number of owners for hash map elements (int64)
func (jm *JMap) CountInt64() (int64, error) {
	return 0, errors.New("not implemented")
	//return hm2.OwnerSet().CountInt64()
}

// DelKey removes a key of an owner in a hashmap (for instance the email field for a user)
func (jm *JMap) DelKey(owner, key string) error {
	return errors.New("not implemented")
	// The key is not removed from the set of all encountered properties
	// even if it's the last key with that name, for a performance vs storage tradeoff.
	//return hm2.KeyValue().Del(owner + fieldSep + key)
}

// Del removes an element (for instance a user)
func (jm *JMap) Del(owner string) error {
	return errors.New("not implemented")

	// 	ownerSet := hm2.OwnerSet()
	// 	items, err := ownerSet.All()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, v := range items {
	// 		if v == owner {
	// 			return ownerSet.Del(v)
	// 		}
	// 	}
	// 	return fmt.Errorf("could not find %s", owner)

}

// Remove this hashmap
func (jm *JMap) Remove() error {
	return errors.New("not implemented")

	// 	hm2.OwnerSet().Remove()
	// 	hm2.PropSet().Remove()
	// 	if err := hm2.KeyValue().Remove(); err != nil {
	// 		return fmt.Errorf("could not remove kv: %s", err)
	// 	}
	// 	return nil

}

// Clear the contents
func (jm *JMap) Clear() error {
	return errors.New("not implemented")

	// 	hm2.OwnerSet().Clear()
	// 	hm2.PropSet().Clear()
	// 	if err := hm2.KeyValue().Clear(); err != nil {
	// 		return err
	// 	}
	// 	return nil

}
