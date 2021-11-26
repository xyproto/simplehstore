package simplehstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
func (hm2 *HashMap2) setPropWithTransaction(ctx context.Context, transaction *sql.Tx, owner, key, value string) error {
	if strings.Contains(key, fieldSep) {
		return fmt.Errorf("key can not contain %s", fieldSep)
	}
	// Add the key to the property set
	if err := hm2.PropSet().addWithTransaction(ctx, transaction, key); err != nil {
		return err
	}
	// Set a key + value for this "owner¤key"
	return hm2.KeyValue().setWithTransaction(ctx, transaction, owner+fieldSep+key, value)
}

// SetMap will set many keys/values, in a single transaction
func (hm2 *HashMap2) SetMap(owner string, m map[string]string) error {

	// Use a context and a transaction to bundle queries
	ctx := context.Background()
	transaction, err := hm2.host.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Check the owner string
	if strings.Contains(owner, fieldSep) {
		return fmt.Errorf("owner can not contain %s", fieldSep)
	}
	// Add the owner to the set
	if err := hm2.OwnerSet().addWithTransaction(ctx, transaction, owner); err != nil {
		return err
	}

	// Prepare the changes
	for k, v := range m {
		if err := hm2.setPropWithTransaction(ctx, transaction, owner, k, v); err != nil {
			transaction.Rollback()
			return err
		}
	}
	return transaction.Commit()
}

func nonexisting(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasSuffix(err.Error(), "does not exist")
}

// Get a value
func (hm2 *HashMap2) Get(owner, key string) (string, error) {
	s, err := hm2.KeyValue().Get(owner + fieldSep + key)
	if err != nil && nonexisting(err) {
		return s, err
	}
	if s == "" {
		err = errors.New("returned value is blank")
		return s, err
	}
	return s, nil
}

// Has checks if a given owner + key exists in the hash map
func (hm2 *HashMap2) Has(owner, key string) (bool, error) {
	s, err := hm2.KeyValue().Get(owner + fieldSep + key) // interpret every error as "row not found", for now
	if nonexisting(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if s == "" {
		return false, nil
	}
	return true, nil
}

// Exists checks if a given owner exists as a hash map at all
func (hm2 *HashMap2) Exists(owner string) (bool, error) {
	if hasOwner, err := hm2.OwnerSet().Has(owner); !nonexisting(err) {
		return hasOwner, err
	}
	return false, nil
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
		if found, err := hm2.Has(owner, key); err == nil && found {
			allKeys = append(allKeys, key)
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
