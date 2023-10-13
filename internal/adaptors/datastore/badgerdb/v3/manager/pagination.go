package manager

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

// ValidatePagination validates whether there is a valid pagination.
func (m *BadgerLocalManager) ValidatePagination(size, page int) (bool, error) {
	if size == 0 && page == 0 {
		return false, nil
	}

	if size == 0 && page != 0 || size != 0 && page == 0 {
		return false,
			fmt.Errorf(
				"invalid pagination - size and page must both be zero or different from zero",
			)
	}

	return true, nil
}

// Paginate traverses the iterator stack until
// the point to where it was requested.
func (m *BadgerLocalManager) Paginate(
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
