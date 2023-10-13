package management_models

import (
	"time"

	"github.com/dgraph-io/badger/v3"
)

type (
	DBInfo struct {
		Name         string    `json:"name,omitempty"`
		Path         string    `json:"path,omitempty"`
		CreatedAt    time.Time `json:"createdAt"`
		LastOpenedAt time.Time `json:"lastOpenedAt"`
	}

	DBMemoryInfo struct {
		Name         string     `json:"name,omitempty"`
		Path         string     `json:"path,omitempty"`
		CreatedAt    time.Time  `json:"createdAt"`
		LastOpenedAt time.Time  `json:"lastOpenedAt"`
		DB           *badger.DB `json:"db,omitempty"`
	}
)

func NewDBInfo(name, path string) *DBInfo {
	dbInfo := &DBInfo{
		Path:         path,
		Name:         name,
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Time{},
	}

	return dbInfo
}

func (i *DBInfo) InfoToMemoryInfo(db *badger.DB) *DBMemoryInfo {
	dbMemInfo := &DBMemoryInfo{
		Name:         i.Name,
		Path:         i.Path,
		CreatedAt:    i.CreatedAt,
		LastOpenedAt: i.LastOpenedAt,
		DB:           db,
	}

	return dbMemInfo
}

func (mi DBMemoryInfo) MemoryInfoToInfo() *DBInfo {
	return &DBInfo{
		Name:         mi.Name,
		Path:         mi.Path,
		CreatedAt:    mi.CreatedAt,
		LastOpenedAt: mi.LastOpenedAt,
	}
}

// CreateStoreRequest holds the payload data to create a store database table.
type CreateStoreRequest struct {
	Name string `json:"name,omitempty" validation:"required"`
	Path string `json:"path,omitempty" validation:"required"`
}

// DeleteStoreRequest holds the payload data to create a store database table.
type DeleteStoreRequest struct {
	Name string `json:"name,omitempty" validation:"required"`
}

// GetStoreRequest holds the payload data to create a store database table.
type GetStoreRequest struct {
	Name string `json:"name,omitempty" validation:"required"`
}

type Pagination struct {
	PageID   uint64 `json:"page_id,omitempty"`
	PageSize uint64 `json:"page_size,omitempty"`
}
