package simplehstore

import (
	"github.com/xyproto/pinterface"
)

// For implementing pinterface.ICreator

type PostgresCreator struct {
	host *Host
}

func NewCreator(host *Host) *PostgresCreator {
	return &PostgresCreator{host}
}

func (m *PostgresCreator) NewList(id string) (pinterface.IList, error) {
	return NewList(m.host, id)
}

func (m *PostgresCreator) NewSet(id string) (pinterface.ISet, error) {
	return NewSet(m.host, id)
}

func (m *PostgresCreator) NewHashMap(id string) (pinterface.IHashMap, error) {
	return NewHashMap(m.host, id)
}

func (m *PostgresCreator) NewKeyValue(id string) (pinterface.IKeyValue, error) {
	return NewKeyValue(m.host, id)
}
