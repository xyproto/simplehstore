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

const (
	listname     = "testlist"
	setname      = "testset"
	hashmapname  = "testhashmap"
	keyvaluename = "testkeyvalue"
	testdata1    = "abc123"
	testdata2    = "def456"
	testdata3    = "ghi789"
)

func TestLocalConnection(t *testing.T) {
	Verbose = true

	//err := TestConnection() // locally
	err := TestConnectionHost("postgres:@127.0.0.1/") // for travis-ci
	//err := TestConnectionHost("go:go@/main") // laptop
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestList1(t *testing.T) {
	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()
	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}
	items, err := list.All()
	if err != nil {
		t.Errorf("Error when retrieving list! %s", err.Error())
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong list length! %v", len(items))
	}
	if (len(items) > 0) && (items[0] != testdata1) {
		t.Errorf("Error, wrong list contents! %v", items)
	}
	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}
	if err := list.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}
	items, err = list.All()
	if err != nil {
		t.Errorf("Error when retrieving list! %s", err.Error())
	}
	if len(items) != 3 {
		t.Errorf("Error, wrong list length! %v", len(items))
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err.Error())
	}
}

func TestList2(t *testing.T) {
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()
	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}
	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}

	item, err := list.GetLast()
	if err != nil {
		t.Errorf("Error, could not get last item from list! %s", err.Error())
	}
	if item != testdata2 {
		t.Errorf("Error, expected %s, got %s with GetLast()!", testdata2, item)
	}

	items, err := list.GetLastN(2)
	if err != nil {
		t.Errorf("Error, could not get last N items from list! %s", err.Error())
	}
	if len(items) != 2 {
		t.Errorf("Error, wrong list length! %v", len(items))
	}
	if items[0] != testdata1 {
		t.Errorf("Error, expected %s, got %s with GetLastN(2)[0]!", testdata1, items[0])
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err.Error())
	}

	// Check that list qualifies for the IList interface
	var _ pinterface.IList = list
}

func TestSet(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

	defer host.Close()
	set, err := NewSet(host, setname)
	if err != nil {
		t.Error(err)
	}
	set.Clear()
	if err := set.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	items, err := set.All()
	if err != nil {
		t.Errorf("Error when retrieving set! %s", err.Error())
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong set length! %v", len(items))
	}
	if (len(items) > 0) && (items[0] != testdata1) {
		t.Errorf("Error, wrong set contents! %v", items)
	}
	if err := set.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	if err := set.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	// Add an element twice. This is a set, so the element should only appear once.
	if err := set.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	items, err = set.All()
	if err != nil {
		t.Errorf("Error when retrieving set! %s", err.Error())
	}
	if len(items) != 3 {
		t.Errorf("Error, wrong set length! %v\n%v\n", len(items), items)
	}
	err = set.Remove()
	if err != nil {
		t.Errorf("Error, could not remove set! %s", err.Error())
	}

	// Check that set qualifies for the ISet interface
	var _ pinterface.ISet = set
}

func TestRawSet(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

	defer host.Close()
	set, err := NewSet(host, setname)
	if err != nil {
		t.Error(err)
	}
	set.Clear()
	if err := set.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	items, err := set.All()
	if err != nil {
		t.Errorf("Error when retrieving set! %s", err.Error())
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong set length! %v", len(items))
	}
	if (len(items) > 0) && (items[0] != testdata1) {
		t.Errorf("Error, wrong set contents! %v", items)
	}
	if err := set.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	if err := set.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	// Add an element twice. This is a set, so the element should only appear once.
	if err := set.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to set! %s", err.Error())
	}
	items, err = set.All()
	if err != nil {
		t.Errorf("Error when retrieving set! %s", err.Error())
	}
	if len(items) != 3 {
		t.Errorf("Error, wrong set length! %v\n%v\n", len(items), items)
	}
	err = set.Remove()
	if err != nil {
		t.Errorf("Error, could not remove set! %s", err.Error())
	}

	// Check that set qualifies for the ISet interface
	var _ pinterface.ISet = set
}

func TestHashMapUserState(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop
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
		t.Errorf("Error when retrieving element! %s", err.Error())
	}
	if aval != "true" {
		t.Error("a should be true")
	}
	bval, err := hashmap.Get(username, "b")
	if err != nil {
		t.Errorf("Error when retrieving elements! %s", err.Error())
	}
	if bval != "true" {
		t.Error("b should be true")
	}
	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err.Error())
	}
}

func TestKeyValue(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

	defer host.Close()
	keyvalue, err := NewKeyValue(host, keyvaluename)
	if err != nil {
		t.Error(err)
	}
	keyvalue.Clear()

	key := "password"
	value := "hunter1"

	if err := keyvalue.Set(key, value); err != nil {
		t.Errorf("Error, could not set value in keyvalue! %s", err.Error())
	}
	// Twice
	if err := keyvalue.Set(key, value); err != nil {
		t.Errorf("Error, could not set value in keyvalue! %s", err.Error())
	}
	item, err := keyvalue.Get(key)
	if err != nil {
		t.Errorf("Error, could not fetch value from keyvalue! %s", err.Error())
	}
	if item != value {
		t.Errorf("Error, expected %s, got %s!", value, item)
	}
	err = keyvalue.Remove()
	if err != nil {
		t.Errorf("Error, could not remove keyvalue! %s", err.Error())
	}
	// Check that keyvalue qualifies for the IKeyValue interface
	var _ pinterface.IKeyValue = keyvalue
}

func TestHashKvMix(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

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
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

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
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}
	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Unable to retrieve from hashmap! %s\n", err.Error())
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
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}
	item, err = hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Unable to retrieve from hashmap! %s\n", err.Error())
	}
	if item != value {
		t.Errorf("Error, got a different value back (sha256)! %s != %s\n", value, item)
	}

	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err.Error())
	}
}

func TestTwoFields(t *testing.T) {
	test, test23, ok := twoFields("test1@test2@test3", "@")
	if ok && ((test != "test1") || (test23 != "test2@test3")) {
		t.Error("Error in twoFields functions")
	}
}

// Check that "bob" is confirmed
func TestConfirmed(t *testing.T) {
	host := NewHost("postgres:@127.0.0.1/")
	defer host.Close()
	users, err := NewHashMap(host, "users")
	if err != nil {
		t.Error(err)
	}
	defer users.Remove()
	users.Set("bob", "confirmed", "true")
	ok, err := users.Exists("bob")
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
	ok, err = users.Has("bob", "confirmed")
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Error("The confirmed key should be gone")
	}
}

func TestDupliSet(t *testing.T) {
	host := NewHost("postgres:@127.0.0.1/")
	defer host.Close()
	letters, err := NewSet(host, "letters")
	if err != nil {
		t.Error(err)
	}
	defer letters.Remove()

	if err := letters.Add("a"); err != nil {
		t.Error(err)
	}
	if err := letters.Add("a"); err != nil {
		t.Error(err)
	}
	x, err := letters.All()
	if err != nil {
		t.Error(err)
	}
	if len(x) != 1 {
		t.Error("The set should have length 1 after adding two identical items")
	}
	if err := letters.Add("b"); err != nil {
		t.Error(err)
	}
	y, err := letters.All()
	if err != nil {
		t.Error(err)
	}
	if len(y) != 2 {
		t.Error("The set should have length 2 after adding two identical items")
	}
}

func TestInc(t *testing.T) {
	Verbose = true
	const (
		kvname     = "kv_234_test_test_test"
		testkey    = "key_234_test_test_test"
		testvalue0 = "9"
		testvalue1 = "10"
		testvalue2 = "1"
	)
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	defer host.Close()
	kv, err := NewKeyValue(host, kvname)
	if err != nil {
		t.Error(err)
	}

	if err := kv.Set(testkey, testvalue0); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue0 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue0)
	}
	incval, err := kv.Inc(testkey)
	if err != nil {
		t.Errorf("Error, could not INCR key! %s", err.Error())
	}
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue1 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue1)
	} else if incval != testvalue1 {
		t.Errorf("Error, wrong inc value! %s != %s", incval, testvalue1)
	}
	kv.Remove()
	if _, err := kv.Get(testkey); err == nil {
		t.Errorf("Error, could get key! %s", err.Error())
	}
	// Creates "0" and increases the value with 1
	kv.Inc(testkey)
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue2 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue2)
	}
	kv.Remove()
	if _, err := kv.Get(testkey); err == nil {
		t.Errorf("Error, could get key! %s", err.Error())
	}
}

func TestInc2(t *testing.T) {
	Verbose = true
	const (
		kvname     = "kv_237_test_test_test_inc"
		testkey    = "key_237_test_test_test_inc"
		emptyvalue = "1"
	)
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	defer host.Close()
	kv, err := NewKeyValue(host, kvname)
	if err != nil {
		t.Error(err)
	}

	kv.Del(testkey)

	if val, err := kv.Inc(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != emptyvalue {
		t.Errorf("Error, wrong value! %s != %s", val, emptyvalue)
	}

	kv.Remove()
}

func TestHashMapUserState2(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

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
		t.Errorf("Key found, when it should be missing! %s", err.Error())
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}

	hashmap.Remove()
}

func TestHashMap(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

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
		t.Errorf("Key found, when it should be missing! %s", err.Error())
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}

	// Once more, with the same data
	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}

	items, err := hashmap.All()
	if err != nil {
		t.Errorf("Error when retrieving elements! %s", err.Error())
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong element length! %v", len(items))
	}

	// Add one more item, so that there shall be 2 items
	if err := hashmap.Set("alice", "number", "42"); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}

	// Retrieve items again and check the length
	items, err = hashmap.All()
	if len(items) != 2 {
		for i, item := range items {
			fmt.Printf("ITEM %d IS %v\n", i, item)
		}
		t.Errorf("Error, wrong element length! %v", len(items))
	}

	if (len(items) > 0) && (items[0] != username) {
		t.Errorf("Error, wrong elementid! %v", items)
	}
	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Error, could not fetch value from hashmap! %s", err.Error())
	}
	if item != value {
		t.Errorf("Error, expected %s, got %s!", value, item)
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
		t.Errorf("Error, could not remove hashmap! %s", err.Error())
	}

	// Check that hashmap qualifies for the IHashMap interface
	var _ pinterface.IHashMap = hashmap
}

func TestDashesAndQuotes(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	//host := NewHost("go:go@/main") // laptop

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
		t.Errorf("Key found, when it should be missing! %s", err.Error())
	}

	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}
	// Once more, with the same data
	if err := hashmap.Set(username, key, value); err != nil {
		t.Errorf("Error, could not set value in hashmap! %s", err.Error())
	}
	if _, err := hashmap.All(); err != nil {
		t.Errorf("Error when retrieving elements! %s", err.Error())
	}
	item, err := hashmap.Get(username, key)
	if err != nil {
		t.Errorf("Error, could not fetch value from hashmap! %s", err.Error())
	}
	if item != value {
		t.Errorf("Error, expected %s, got %s!", value, item)
	}
	err = hashmap.Remove()
	if err != nil {
		t.Errorf("Error, could not remove hashmap! %s", err.Error())
	}
}

func TestInc3(t *testing.T) {
	Verbose = true
	const (
		kvname     = "kv_237_test_test_test_inc's-x"
		testkey    = "key_237_test_test_test_inc's-x"
		emptyvalue = "1"
	)
	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	defer host.Close()
	kv, err := NewKeyValue(host, kvname)
	if err != nil {
		t.Error(err)
	}

	kv.Del(testkey)

	if val, err := kv.Inc(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != emptyvalue {
		t.Errorf("Error, wrong value! %s != %s", val, emptyvalue)
	}

	kv.Remove()
}

func TestRemoveItem(t *testing.T) {

	host := NewHost("postgres:@127.0.0.1/") // for travis-ci
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()

	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}

	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}

	err = list.RemoveByIndex(0)
	if err != nil {
		t.Errorf("Error, could not remove item #0! %s", err.Error())
	}

	items, err := list.All()
	if err != nil {
		t.Errorf("Error, could not get items from list! %s", err.Error())
	}

	if len(items) != 1 {
		t.Error("Error, expected there to only be one item in the list!")
	}

	if items[0] != testdata2 {
		t.Errorf("Error, expected %s, got %s with All()!", testdata2, items[0])
	}

	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err.Error())
	}
}
