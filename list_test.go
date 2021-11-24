package simplehstore

import (
	"testing"

	"github.com/xyproto/pinterface"
)

const listname = "testlist"

func TestList1(t *testing.T) {
	//host := New() // locally
	host := NewHost(defaultConnectionString)
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()
	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}
	items, err := list.All()
	if err != nil {
		t.Errorf("Error when retrieving list! %s", err)
	}
	if len(items) != 1 {
		t.Errorf("Error, wrong list length! %d", len(items))
	}
	if (len(items) > 0) && (items[0] != testdata1) {
		t.Errorf("Error, wrong list contents! %v", items)
	}
	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}
	if err := list.Add(testdata3); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}
	items, err = list.All()
	if err != nil {
		t.Errorf("Error when retrieving list! %s", err)
	}
	if len(items) != 3 {
		t.Errorf("Error, wrong list length! %d", len(items))
	}
	if count, err := list.Count(); err != nil {
		t.Errorf("Error when counting list: %s", err)
	} else if count != 3 {
		t.Errorf("Error, wrong list length! %d", count)
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err)
	}
}

func TestList2(t *testing.T) {
	host := NewHost(defaultConnectionString)
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()
	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}
	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}

	item, err := list.GetLast()
	if err != nil {
		t.Errorf("Error, could not get last item from list! %s", err)
	}
	if item != testdata2 {
		t.Errorf("Error, expected %s, got %s with GetLast()!", testdata2, item)
	}

	items, err := list.GetLastN(2)
	if err != nil {
		t.Errorf("Error, could not get last N items from list! %s", err)
	}
	if len(items) != 2 {
		t.Errorf("Error, wrong list length! %d", len(items))
	}
	if items[0] != testdata1 {
		t.Errorf("Error, expected %s, got %s with GetLastN(2)[0]!", testdata1, items[0])
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err)
	}

	// Check that list qualifies for the IList interface
	var _ pinterface.IList = list
}

func TestRemoveItemFromList(t *testing.T) {

	host := NewHost(defaultConnectionString)
	defer host.Close()

	list, err := NewList(host, listname)
	if err != nil {
		t.Error(err)
	}
	list.Clear()

	if err := list.Add(testdata1); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}

	if err := list.Add(testdata2); err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}

	err = list.RemoveByIndex(0)
	if err != nil {
		t.Errorf("Error, could not remove item #0! %s", err)
	}

	items, err := list.All()
	if err != nil {
		t.Errorf("Error, could not get items from list! %s", err)
	}

	if len(items) != 1 {
		t.Error("Error, expected there to only be one item in the list!")
	}

	if items[0] != testdata2 {
		t.Errorf("Error, expected %s, got %s with All()!", testdata2, items[0])
	}

	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err)
	}
}
