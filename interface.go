package db

// Database interfaces

type IList interface {
	Add(value string) error
	GetAll() ([]string, error)
	GetLast() (string, error)
	GetLastN(n int) ([]string, error)
	Remove() error
	Clear() error
}

type ISet interface {
	Add(value string) error
	Has(value string) (bool, error)
	GetAll() ([]string, error)
	Del(value string) error
	Remove() error
	Clear() error
}

type IHashMap interface {
	Set(owner, key, value string) error
	Get(owner, key string) (string, error)
	Has(owner, key string) (bool, error)
	Exists(owner string) (bool, error)
	GetAll() ([]string, error)
	DelKey(owner, key string) error
	Del(key string) error
	Remove() error
	Clear() error
}

type IKeyValue interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Del(key string) error
	Remove() error
	Clear() error
}
