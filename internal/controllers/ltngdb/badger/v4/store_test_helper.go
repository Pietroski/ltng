package badgerdb_controller_v4

import (
	"fmt"
	"reflect"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
)

// A Matcher is a representation of a class of values.
// It is used to represent the valid or expected arguments to a mocked method.
type Matcher interface {
	// Matches returns whether x is a match.
	Matches(x interface{}) bool

	// String describes what the matcher matches.
	String() string
}

type eqDBInfoMatcher struct {
	x interface{}
}

func EqDBInfo(x interface{}) Matcher { return eqDBInfoMatcher{x} }

func (e eqDBInfoMatcher) Matches(x interface{}) bool {
	dbInfo, ok := x.(*badgerdb_management_models_v4.DBInfo)
	if !ok {
		return false
	}

	receivedDbInfo, ok := e.x.(*badgerdb_management_models_v4.DBInfo)
	if !ok {
		return false
	}

	receivedDbInfo.CreatedAt = dbInfo.CreatedAt
	receivedDbInfo.LastOpenedAt = dbInfo.LastOpenedAt

	isEqual := reflect.DeepEqual(dbInfo, receivedDbInfo)

	return isEqual
}

func (e eqDBInfoMatcher) String() string {
	return fmt.Sprintf("is equal to %v (%T)", e.x, e.x)
}

type eqPaginationMatcher struct {
	x interface{}
}

func EqPaginationInfo(x interface{}) Matcher { return eqPaginationMatcher{x} }

func (e eqPaginationMatcher) Matches(x interface{}) bool {
	pagination, ok := x.(*badgerdb_management_models_v4.Pagination)
	if !ok {
		return false
	}

	receivedPagination, ok := e.x.(*badgerdb_management_models_v4.Pagination)
	if !ok {
		return false
	}

	receivedPagination.PageID = pagination.PageID
	receivedPagination.PageSize = pagination.PageSize

	isEqual := reflect.DeepEqual(pagination, receivedPagination)

	return isEqual
}

func (e eqPaginationMatcher) String() string {
	return fmt.Sprintf("is equal to %v (%T)", e.x, e.x)
}
