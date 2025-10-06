package v4

import (
	"github.com/dgraph-io/badger/v4"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

// ValidatePagination validates whether there is a valid pagination.
func (m *BadgerLocalManagerV4) ValidatePagination(size, page int) (bool, error) {
	if size == 0 && page == 0 {
		return false, nil
	}

	if size == 0 && page != 0 || size != 0 && page == 0 {
		return false,
			errorsx.New("invalid pagination - size and page must both be zero or different from zero")
	}

	return true, nil
}

// Paginate traverses the iterator stack until
// the point to where it was requested.
func (m *BadgerLocalManagerV4) Paginate(
	it *badger.Iterator,
	size, page int,
) *badger.Iterator {
	it.Rewind()
	//for number := 1; number < page; number++ {
	//	for i := 0; i < size; i++ {
	//		if it.Valid() {
	//			it.Next()
	//		}
	//	}
	//}

	limit := page * size
	for idx := 1; idx < limit; idx++ {
		if it.Valid() {
			it.Next()
		}
	}

	return it
}
