package db

import (
	"github.com/xyproto/pinterface"
)

// For implementing pinterface.ICreator

type MariaCreator struct {
	host *Host
}

func NewCreator(host *Host) *MariaCreator {
	return &MariaCreator{host}
}

func (m *MariaCreator) NewList(id string) pinterface.IList {
	return NewList(m.host, id)
}

func (m *MariaCreator) NewSet(id string) pinterface.ISet {
	return NewSet(m.host, id)
}

func (m *MariaCreator) NewHashMap(id string) pinterface.IHashMap {
	return NewHashMap(m.host, id)
}

func (m *MariaCreator) NewKeyValue(id string) pinterface.IKeyValue {
	return NewKeyValue(m.host, id)
}
