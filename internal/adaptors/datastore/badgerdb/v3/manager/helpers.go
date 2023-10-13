package manager

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
)

// deserializeItem deserializes a badger item and,
// return its key as string, value as the DBInfo model and,
// and error in case of any previous step's failure.
func (m *BadgerLocalManager) deserializeItem(item *badger.Item) (
	key string,
	value management_models.DBInfo,
	err error,
) {
	serializedKey := item.KeyCopy(nil)
	serializedValue, err := item.ValueCopy(nil)
	if err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to retrieve serialized item value: %v", err)
	}

	if err = m.serializer.Deserialize(serializedKey, &key); err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to deserialize key on start: %v", err)
	}
	if err = m.serializer.Deserialize(serializedValue, &value); err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to deserialize value on start: %v", err)
	}

	return
}
