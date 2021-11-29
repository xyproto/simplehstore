package simplehstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// HashMap2 contains a KeyValue struct and a dbDatastructure.
// Each value is a JSON data blob and can contains sub-keys.
type HashMap2 struct {
	dbDatastructure        // KeyValue is .host *Host + .table string
	ownerTable      string // Set of owner keys
	seenPropTable   string // Set of all encountered property keys
}

// A string that is unlikely to appear in a key
const fieldSep = "¤"

// NewHashMap2 creates a new HashMap2 struct
func NewHashMap2(host *Host, name string) (*HashMap2, error) {
	var hm2 HashMap2
	// kv is a KeyValue (HSTORE) table of all properties (key = owner_ID + "¤" + property_key)
	kv, err := NewKeyValue(host, name+"_properties_HSTORE_map")
	if err != nil {
		return nil, err
	}
	// ownerSet is a set of all stored owners/IDs
	ownerSet, err := NewSet(host, name+"_set_of_all_IDs")
	if err != nil {
		return nil, err
	}
	// seenPropSet is a set of all encountered property keys
	seenPropSet, err := NewSet(host, name+"_encountered_property_keys")
	if err != nil {
		return nil, err
	}
	hm2.host = host
	hm2.table = kv.table
	hm2.ownerTable = ownerSet.table
	hm2.seenPropTable = seenPropSet.table
	return &hm2, nil
}

// KeyValue returns the *KeyValue of properties for this HashMap2
func (hm2 *HashMap2) KeyValue() *KeyValue {
	return &KeyValue{hm2.host, hm2.table}
}

// OwnerSet returns the owner *Set for this HashMap2
func (hm2 *HashMap2) OwnerSet() *Set {
	return &Set{hm2.host, hm2.ownerTable}
}

// PropSet returns the property *Set for this HashMap2
func (hm2 *HashMap2) PropSet() *Set {
	return &Set{hm2.host, hm2.seenPropTable}
}

// Set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (hm2 *HashMap2) Set(owner, key, value string) error {
	return hm2.SetMap(owner, map[string]string{key: value})
}

// setPropWithTransaction will set a value in a hashmap given the element id (for instance a user id) and the key (for instance "password")
func (hm2 *HashMap2) setPropWithTransaction(ctx context.Context, transaction *sql.Tx, owner, key, value string, checkForFieldSep bool) error {
	if checkForFieldSep {
		if strings.Contains(owner, fieldSep) {
			return fmt.Errorf("owner can not contain %s", fieldSep)
		}
		if strings.Contains(key, fieldSep) {
			return fmt.Errorf("key can not contain %s", fieldSep)
		}
	}
	// Add the key to the property set
	if err := hm2.PropSet().addWithTransaction(ctx, transaction, key); err != nil {
		return err
	}
	// Set a key + value for this "owner¤key"
	kv := hm2.KeyValue()
	if !kv.host.rawUTF8 {
		Encode(&value)
	}
	encodedValue := value
	return kv.setWithTransaction(ctx, transaction, owner+fieldSep+key, encodedValue)
}

// SetMap will set many keys/values, in a single transaction
func (hm2 *HashMap2) SetMap(owner string, m map[string]string) error {
	checkForFieldSep := true

	// Get all properties
	propset := hm2.PropSet()
	allProperties, err := propset.All()
	if err != nil {
		return err
	}

	// Use a context and a transaction to bundle queries
	ctx := context.Background()
	transaction, err := hm2.host.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Add the owner to the set
	if err := hm2.OwnerSet().addWithTransaction(ctx, transaction, owner); err != nil {
		transaction.Rollback()
		return err
	}

	// Prepare the changes
	for k, v := range m {
		if err := hm2.setPropWithTransaction(ctx, transaction, owner, k, v, checkForFieldSep); err != nil {
			transaction.Rollback()
			return err
		}
		if !hasS(allProperties, k) {
			if err := propset.addWithTransaction(ctx, transaction, k); err != nil {
				transaction.Rollback()
				return err
			}
		}
	}
	return transaction.Commit()
}

// SetLargeMap will add many owners+keys/values, in a single transaction, without checking if they already exists.
// It also does not check if the keys or property keys contains fieldSep (¤) or not, for performance.
// These must all be brand new "usernames" (the first key), and not be in the existing hm2.OwnerSet().
// This function has good performance, but must be used carefully.
func (hm2 *HashMap2) SetLargeMap(allProperties map[string]map[string]string) error {
	var (
		insertErr, propSetErr, updateErr error
		wg                               sync.WaitGroup
	)

	if Verbose {
		fmt.Println("SetLargeMap START")
	}

	// First get the KeyValue and Set structures that will be used
	ownerSet := hm2.OwnerSet()
	kv := hm2.KeyValue()
	propSet := hm2.PropSet()

	// Get all existing owners
	allOwners, err := ownerSet.All()
	if err != nil {
		return err
	}

	if Verbose {
		fmt.Printf("Got all %d owners\n", len(allOwners))
	}

	// Find all unique properties
	props := []string{}

	// Find all owners in allProperties that already exists, and those that doesn't.
	recognizedOwners := []string{}
	unrecognizedOwners := []string{}
	for owner := range allProperties {
		if hasS(allOwners, owner) {
			recognizedOwners = append(recognizedOwners, owner)
		} else {
			unrecognizedOwners = append(unrecognizedOwners, owner)
		}
		// Find all unique properties
		for k := range allProperties[owner] {
			if !hasS(props, k) {
				props = append(props, k)
			}
		}
	}

	ctx := context.Background()

	wg.Add(1)
	go func() {
		defer wg.Done()

		if Verbose {
			fmt.Println("prop set START")
		}

		// Create a new transaction
		transaction, err := hm2.host.db.BeginTx(ctx, nil)
		if err != nil {
			propSetErr = err
			return
		}

		// Store all properties (should be a low number)
		for _, prop := range props {
			if Verbose {
				fmt.Printf("ADDING %s\n", prop)
			}
			if err := propSet.addWithTransaction(ctx, transaction, prop); err != nil {
				propSetErr = err
				return
			}
		}

		if Verbose {
			fmt.Println("prop set COMMIT")
		}

		// And send it
		propSetErr = transaction.Commit()

		if Verbose {
			fmt.Println("prop set DONE")
		}
	}()

	// Start one goroutine + transaction for the recognized owners
	wg.Add(1)
	go func() {
		defer wg.Done()

		if Verbose {
			fmt.Println("recognized owners START")
		}

		// Create a new transaction
		transaction, err := hm2.host.db.BeginTx(ctx, nil)
		if err != nil {
			updateErr = err
			return
		}

		// Then update all recognized owners
		for _, owner := range recognizedOwners {
			// Prepare the changes
			for k, value := range allProperties[owner] {
				if Verbose {
					fmt.Printf("SETTING %s %s->%s\n", owner, k, value)
				}
				// Set a key + value for this "owner¤key"
				if !kv.host.rawUTF8 {
					Encode(&value)
				}
				encodedValue := value
				if _, err := kv.updateWithTransaction(ctx, transaction, owner+fieldSep+k, encodedValue); err != nil {
					transaction.Rollback()
					updateErr = err
					return
				}
			}
		}

		if Verbose {
			fmt.Println("recognized owners COMMIT")
		}

		// And send it
		updateErr = transaction.Commit()

		if Verbose {
			fmt.Println("recognized owners DONE")
		}
	}()

	// Start one goroutine + transaction for the unrecognized owners
	wg.Add(1)
	go func() {
		defer wg.Done()

		if Verbose {
			fmt.Println("unrecognized owners START")
		}

		// Create a new transaction
		transaction, err := hm2.host.db.BeginTx(ctx, nil)
		if err != nil {
			insertErr = err
			return
		}

		// Then update all unrecognized owners
		for _, owner := range unrecognizedOwners {
			// Add the owner to the set of owners in the database
			if err := ownerSet.addWithTransaction(ctx, transaction, owner); err != nil {
				transaction.Rollback()
				insertErr = err
				return
			}
			// Prepare the changes
			for k, value := range allProperties[owner] {
				if Verbose {
					fmt.Printf("SETTING %s %s->%s\n", owner, k, value)
				}
				// Set a key + value for this "owner¤key"
				if !kv.host.rawUTF8 {
					Encode(&value)
				}
				encodedValue := value
				if _, err := kv.updateWithTransaction(ctx, transaction, owner+fieldSep+k, encodedValue); err != nil {
					transaction.Rollback()
					insertErr = err
					return
				}
			}
		}

		if Verbose {
			fmt.Println("unrecognized owners COMMIT")
		}

		// And send it
		insertErr = transaction.Commit()

		if Verbose {
			fmt.Println("unrecognized owners DONE")
		}
	}()

	wg.Wait()

	if Verbose {
		fmt.Println("SetLargeMap DONE")
	}

	// Check the error values
	if updateErr != nil {
		return updateErr
	}
	if insertErr != nil {
		return insertErr
	}
	if propSetErr != nil {
		return propSetErr
	}
	return nil // success
}

// Get a value.
// Returns: value, error
// If a value was not found, an empty string is returned.
func (hm2 *HashMap2) Get(owner, key string) (string, error) {
	s, err := hm2.KeyValue().Get(owner + fieldSep + key)
	if err != nil {
		if noResult(err) {
			return "", nil
		}
		return "", err
	}
	// No error and no value
	if s == "" {
		return s, nil
	}
	// No error and actually got a value
	return s, nil
}

// Get multiple values
func (hm2 *HashMap2) GetMap(owner string, keys []string) (map[string]string, error) {
	results := make(map[string]string)

	// Use a context and a transaction to bundle queries
	ctx := context.Background()
	transaction, err := hm2.host.db.BeginTx(ctx, nil)
	if err != nil {
		return results, err
	}

	for _, key := range keys {
		s, err := hm2.KeyValue().getWithTransaction(ctx, transaction, owner+fieldSep+key)
		if err != nil {
			transaction.Rollback()
			return results, err
		}
		results[key] = s
	}

	transaction.Commit()
	return results, nil
}

// Has checks if a given owner + key exists in the hash map
func (hm2 *HashMap2) Has(owner, key string) (bool, error) {
	s, err := hm2.KeyValue().Get(owner + fieldSep + key)
	if err != nil {
		if noResult(err) {
			// Not an actual error, just got no results
			return false, nil
		}
		// An actual error
		return false, err
	}
	// No error, got a result
	if s == "" {
		return false, nil
	}
	return true, nil
}

// Exists checks if a given owner exists as a hash map at all
func (hm2 *HashMap2) Exists(owner string) (bool, error) {
	found, err := hm2.OwnerSet().Has(owner)
	if err != nil {
		// Either an actual error or no result
		if noResult(err) {
			return false, nil
		}
		// An actual error
		return false, err
	}
	// Got a result, no error
	return found, nil
}

// AllWhere returns all owner ID's that has a property where key == value
func (hm2 *HashMap2) AllWhere(key, value string) ([]string, error) {
	allOwners, err := hm2.OwnerSet().All()
	if err != nil {
		return []string{}, err
	}
	// TODO: Improve the performance of this by using SQL instead of looping
	foundOwners := []string{}
	for _, owner := range allOwners {
		// The owner+key exists and the value matches the given value
		if v, err := hm2.Get(owner, key); err == nil && v == value {
			foundOwners = append(foundOwners, owner)
		}
	}
	return foundOwners, nil
}

// Keys loops through absolutely all owners and all properties in the database
// and returns all found keys.
func (hm2 *HashMap2) Keys(owner string) ([]string, error) {
	allProps, err := hm2.PropSet().All()
	if err != nil {
		return []string{}, err
	}
	// TODO: Improve the performance of this by using SQL instead of looping
	allKeys := []string{}
	for _, key := range allProps {
		fmt.Printf("HAS %s GOT %s? ", owner, key)
		if found, err := hm2.Has(owner, key); err == nil && found {
			fmt.Printf("YES\n")
			allKeys = append(allKeys, key)
		} else {
			fmt.Printf("NO\n")
		}
	}
	return allKeys, nil
}

// All returns all owner ID's
func (hm2 *HashMap2) All() ([]string, error) {
	return hm2.OwnerSet().All()
}

// Count counts the number of owners for hash map elements
func (hm2 *HashMap2) Count() (int, error) {
	return hm2.OwnerSet().Count()
}

// CountInt64 counts the number of owners for hash map elements (int64)
func (hm2 *HashMap2) CountInt64() (int64, error) {
	return hm2.OwnerSet().CountInt64()
}

// DelKey removes a key of an owner in a hashmap (for instance the email field for a user)
func (hm2 *HashMap2) DelKey(owner, key string) error {
	// The key is not removed from the set of all encountered properties
	// even if it's the last key with that name, for a performance vs storage tradeoff.
	return hm2.KeyValue().Del(owner + fieldSep + key)
}

// Del removes an element (for instance a user)
func (hm2 *HashMap2) Del(owner string) error {
	ownerSet := hm2.OwnerSet()
	items, err := ownerSet.All()
	if err != nil {
		return err
	}
	for _, v := range items {
		if v == owner {
			return ownerSet.Del(v)
		}
	}
	return fmt.Errorf("could not find %s", owner)
}

// Remove this hashmap
func (hm2 *HashMap2) Remove() error {
	hm2.OwnerSet().Remove()
	hm2.PropSet().Remove()
	if err := hm2.KeyValue().Remove(); err != nil {
		return fmt.Errorf("could not remove kv: %s", err)
	}
	return nil
}

// Clear the contents
func (hm2 *HashMap2) Clear() error {
	hm2.OwnerSet().Clear()
	hm2.PropSet().Clear()
	if err := hm2.KeyValue().Clear(); err != nil {
		return err
	}
	return nil
}
