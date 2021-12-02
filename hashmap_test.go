package simplehstore

import (
	"fmt"
	"testing"

	// For testing the storage of bcrypt password hashes
	"golang.org/x/crypto/bcrypt"

	"crypto/sha256"
	"io"

	"github.com/xyproto/cookie"
	"github.com/xyproto/pinterface"
)

const hashmapname = "testhashmap"

func TestHashMapUserStateShort(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()
	hashmap, err := NewHashMap(host, hashmapname)
	if err != nil {
		t.Error(err)
	}

	hashmap.Clear()

	username := "bob"

	err = hashmap.Set(username, "aa", "true")
	if err != nil {
		t.Error(err)
	}

	err = hashmap.Set(username, "aa", "false")
	if err != nil {
		t.Error(err)
	}

	err = hashmap.Set(username, "bb", "82")
	if err != nil {
		t.Error(err)
	}

	var existed bool
	existed, err = hashmap.SetCheck(username, "bb", "42")
	if err != nil {
		t.Error(err)
	}
	if !existed {
		t.Error("got wrong bool, the key already existed")
	}

	aval, err := hashmap.Get(username, "aa")
	if err != nil {
		t.Error(err)
	}
	if aval != "false" {
		t.Error("aa should be false, but it is: " + aval)
	}

	json, err := hashmap.json(username)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(json)

	err = hashmap.CreateIndexTable()
	if err != nil {
		t.Error(err)
	}

	json, err = hashmap.json(username)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(json)

	keys, err := hashmap.All()
	if err != nil {
		t.Error(err)
	}
	if len(keys) != 1 {
		t.Errorf("expected 1 username, got: %v", keys)
	}

	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err)
	}
}

func TestHashMapUserState(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()

	hashmap, err := NewHashMap(host, hashmapname)
	if err != nil {
		t.Error(err)
	}
	hashmap.Clear()

	username := "bob"

	err = hashmap.Set(username, "a", "true")
	if err != nil {
		t.Error(err)
	}

	err = hashmap.Set(username, "a", "false")
	if err != nil {
		t.Error(err)
	}

	aval, err := hashmap.Get(username, "a")
	if err != nil {
		t.Error(err)
	}
	if aval != "false" {
		t.Error("a should be false")
	}

	err = hashmap.Set(username, "a", "true")
	if err != nil {
		t.Error(err)
	}

	err = hashmap.Set(username, "b", "true")
	if err != nil {
		t.Error(err)
	}

	err = hashmap.Set(username, "b", "true")
	if err != nil {
		t.Error(err)
	}

	aval, err = hashmap.Get(username, "a")
	if err != nil {
		t.Errorf("Error when retrieving element! %s", err)
	}
	if aval != "true" {
		t.Error("a should be true")
	}

	bval, err := hashmap.Get(username, "b")
	if err != nil {
		t.Errorf("Error when retrieving elements! %s", err)
	}
	if bval != "true" {
		t.Error("b should be true")
	}

	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err)
	}

}

func TestHashKvMix(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()

	sameName := "ostekake"

	h, err := NewHashMap(host, sameName)
	if err != nil {
		t.Error(err)
	}
	h.Set("a", "b", "c")
	defer h.Remove()

	kv, err := NewKeyValue(host, sameName)
	if err != nil {
		t.Error(err)
	}
	kv.Remove()

	v, err := h.Get("a", "b")
	if err != nil {
		t.Error(err)
	}

	if v != "c" {
		t.Errorf("Error, hashmap table name collision")
	}
}

func TestHashStorage(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()
	hashmap, err := NewHashMap(host, hashmapname)
	if err != nil {
		t.Error(err)
	}
	hashmap.Clear()

	username := "bob"
	key := "password"
	password := "hunter1"

	// bcrypt test

	passwordHash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	value := string(passwordHash)

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}
	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Unable to retrieve from hashmap! %s\n", err)
	}
	if item != value {
		t.Errorf("Error, got a different value back (bcrypt)! %s != %s\n", value, item)
	}

	// sha256 test

	hasher := sha256.New()
	io.WriteString(hasher, password+cookie.RandomCookieFriendlyString(30)+username)
	passwordHash = hasher.Sum(nil)
	value = string(passwordHash)

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}
	item, err = hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Unable to retrieve from hashmap! %s\n", err)
	}
	if item != value {
		t.Errorf("Error, got a different value back (sha256)! %s != %s\n", value, item)
	}

	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err)
	}
}

// Check that "bob" is confirmed
func TestConfirmed(t *testing.T) {
	host := NewHost(defaultConnectionString)
	defer host.Close()
	users, err := NewHashMap(host, "users")
	if err != nil {
		t.Error(err)
	}
	defer users.Remove()
	ok, err := users.Exists("bob")
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Error("bob should not exist!")
	}
	users.Set("bob", "confirmed", "true")
	ok, err = users.Exists("bob")
	if err != nil {
		t.Error(err)
	}
	if !ok {
		t.Error("bob should exist!")
	}
	val, err := users.Get("bob", "confirmed")
	if err != nil {
		t.Error(err)
	}
	if val != "true" {
		t.Error("bob should be confirmed")
	}
	err = users.DelKey("bob", "confirmed")
	if err != nil {
		t.Error(err)
	}
	if ok, err := users.Has("bob", "confirmed"); err != nil {
		if noResult(err) {
			fmt.Println("good")
		} else {
			t.Error(err)
		}
	} else if ok {
		t.Error("The confirmed key should be gone")
	}
}

func TestHashMapUserState2(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()
	hashmap, err := NewHashMap(host, hashmapname)
	if err != nil {
		t.Error(err)
	}
	hashmap.Clear()

	username := "bob"
	key := "password"
	value := "hunter1"

	// Get key that doesn't exist yet
	_, err = hashmap.Get("ownerblabla", "keyblabla")
	if err == nil {
		t.Errorf("Key found, when it should be missing! %s", err)
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	hashmap.Remove()
}

func TestHashMap1(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()
	hashmap, err := NewHashMap(host, hashmapname)
	if err != nil {
		t.Error(err)
	}
	hashmap.Clear()

	username := "bob"
	key := "password"
	value := "hunter1"

	// Get key that doesn't exist yet
	_, err = hashmap.Get("ownerblabla", "keyblabla")
	if err == nil {
		t.Errorf("Key found, when it should be missing! %s", err)
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	// Once more, with the same data
	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	items, err := hashmap.All()
	if err != nil {
		t.Errorf("Error when retrieving elements! %s", err)
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong element length! %d", len(items))
	}

	if err := hashmap.Set("bob", "number", "64"); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	if err := hashmap.Set("alice", "number", "128"); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	// Add one more item, so that there are 3 entries in the database,
	// two with owner "bob" and 1 with owner "alice"
	if err := hashmap.Set("alice", "number", "42"); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}

	// Retrieve items again and check the length
	items, err = hashmap.All()
	if err != nil {
		t.Errorf("Error, could not retrieve all items! %s", err)
	}
	if len(items) != 2 {
		for i, item := range items {
			fmt.Printf("ITEM %d IS %v\n", i, item)
		}
		t.Errorf("Error, wrong element length! %d", len(items))
	}

	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Error, could not fetch value from hashmap! %s", err)
	}
	if item != value {
		t.Errorf("Error, expected %s, got %s!", value, item)
	}

	count, err := hashmap.Count()
	if err != nil {
		t.Error("Error, could not get the count!")
	}
	if count != 2 {
		t.Errorf("Error, expected the count of bob and alice to be 2, got %d!", count)
	}

	items, err = hashmap.AllWhere("number", "64")
	if err != nil {
		t.Error("Error, could not get value for property number")
	}
	if len(items) != 1 {
		t.Errorf("Error, there should be only one entry where the number is 64, but we got: %v", items)
	}
	fmt.Println("Items where number is 64:", items)

	// Delete the "number" property/key from owner "bob"
	err = hashmap.DelKey("bob", "number")
	if err != nil {
		t.Error(err)
	}

	// Delete the "number" property/key from owner "alice"
	err = hashmap.DelKey("alice", "number")
	if err != nil {
		t.Error(err)
	}

	keys, err := hashmap.Keys(username)
	if err != nil {
		t.Error(err)
	}
	// only "password"
	if len(keys) != 1 {
		t.Errorf("Error, wrong keys: %v\n", keys)
	}
	if keys[0] != "password" {
		t.Errorf("Error, wrong keys: %v\n", keys)
	}

	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err)
	}

	// Check that hashmap qualifies for the IHashMap interface
	var _ pinterface.IHashMap = hashmap
}

func TestDashesAndQuotes(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

	defer host.Close()
	hashmap, err := NewHashMap(host, hashmapname+"'s-")
	if err != nil {
		t.Error(err)
	}
	hashmap.Clear()

	username := "bob's kitchen-machine"
	key := "password"
	value := "hunter's table-cloth"

	// Get key that doesn't exist yet
	_, err = hashmap.Get("ownerblabla", "keyblabla")
	if err == nil {
		t.Errorf("Key found, when it should be missing! %s", err)
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}
	// Once more, with the same data
	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err)
	}
	if _, err := hashmap.All(); err != nil {
		t.Errorf("Error when retrieving elements! %s", err)
	}
	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Error, could not fetch value from hashmap! %s", err)
	}
	if item != value {
		t.Errorf("Error, expected %s, got %s!", value, item)
	}
	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err)
	}
}
