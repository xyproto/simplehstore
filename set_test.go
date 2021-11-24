package simplehstore

import (
	"testing"

	"github.com/xyproto/pinterface"
)

const setname = "testset"

func TestSet(t *testing.T) {
	Verbose = true

	//host := New() // locally
	host := NewHost(defaultConnectionString)

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
	host := NewHost(defaultConnectionString)

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

func TestDupliSet(t *testing.T) {
	host := NewHost(defaultConnectionString)
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
		t.Error("The set should have length 2 after adding two different items")
	}
}

func TestCount(t *testing.T) {
	host := NewHost(defaultConnectionString)
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
	if count, err := letters.Count(); err != nil {
		t.Error(err)
	} else if count != 1 {
		t.Error("The set should have length 1 after adding two identical items")
	}
	if err := letters.Add("b"); err != nil {
		t.Error(err)
	}
	if count, err := letters.Count(); err != nil {
		t.Error(err)
	} else if count != 2 {
		t.Error("The set should have length 2 after adding two different items")
	}
}
