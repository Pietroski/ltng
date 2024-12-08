package badgerdb_manager_adaptor_v4

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
)

// deserializeItem deserializes a badger item and,
// return its key as string, value as the DBInfo model and,
// and error in case of any previous step's failure.
func (m *BadgerLocalManagerV4) deserializeItem(item *badger.Item) (
	key string,
	value badgerdb_management_models_v4.DBInfo,
	err error,
) {
	serializedKey := item.KeyCopy(nil)
	serializedValue, err := item.ValueCopy(nil)
	if err != nil {
		return "",
			badgerdb_management_models_v4.DBInfo{},
			fmt.Errorf("failed to retrieve serialized item value: %v", err)
	}

	if err = m.serializer.Deserialize(serializedKey, &key); err != nil {
		return "",
			badgerdb_management_models_v4.DBInfo{},
			fmt.Errorf("failed to deserialize key on start: %v", err)
	}
	if err = m.serializer.Deserialize(serializedValue, &value); err != nil {
		return "",
			badgerdb_management_models_v4.DBInfo{},
			fmt.Errorf("failed to deserialize value on start: %v", err)
	}

	return
}
