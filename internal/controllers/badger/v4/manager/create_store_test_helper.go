package badgerdb_manager_controller_v4

import (
	"fmt"
	"reflect"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
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
