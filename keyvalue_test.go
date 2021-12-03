package simplehstore

import (
	"testing"

	"github.com/xyproto/pinterface"
)

const keyvaluename = "testkeyvalue"

func TestKeyValue(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

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

func TestInc(t *testing.T) {
	Verbose = true
	const (
		kvname     = "kv_234_test_test_test"
		testkey    = "key_234_test_test_test"
		testvalue0 = "9"
		testvalue1 = "10"
		testvalue2 = "1"
	)
	host := NewHost(defaultConnectionString)
	defer host.Close()
	kv, err := NewKeyValue(host, kvname)
	if err != nil {
		t.Error(err)
	}

	if err := kv.Set(testkey, testvalue0); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}
	if err := kv.Set(testkey+"2", testvalue0); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}
	if err := kv.Set(testkey+"3", testvalue0); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}

	allValues, err := kv.All()
	if err != nil {
		t.Error(err)
	}
	if len(allValues) != 3 {
		t.Error("should be 3 keys")
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
	host := NewHost(defaultConnectionString)
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

func TestInc3(t *testing.T) {
	Verbose = true
	const (
		kvname     = "kv_237_test_test_test_inc's-x"
		testkey    = "key_237_test_test_test_inc's-x"
		emptyvalue = "1"
	)
	host := NewHost(defaultConnectionString)
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
