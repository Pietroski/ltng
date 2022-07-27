package sqlc_user_store

import (
	"database/sql"
)

// Store provides all functions to execute db queries
type Store interface {
	Querier
}

type userStore struct {
	*Queries
	db *sql.DB
}

// NewUserStore instantiates a devices store object returning the store interface.
func NewUserStore(db *sql.DB) Store {
	store := &userStore{
		Queries: New(db),
		db:      db,
	}

	return store
}
