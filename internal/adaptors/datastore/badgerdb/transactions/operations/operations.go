package operations

import (
	"github.com/dgraph-io/badger/v3"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/models/operation"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

type (
	Operator interface {
		Operate(dbInfo *management_models.DBMemoryInfo) Operator

		Create(key, value []byte) error
		Update(key, value []byte) error
		Load(key []byte) ([]byte, error)
		Delete(key []byte) error

		ListAll() (operation_models.OpList, error)
		ListPaginated(
			pagination *management_models.PaginationRequest,
		) (operation_models.OpList, error)
	}

	BadgerOperator struct {
		manager    manager.Manager
		dbInfo     *management_models.DBMemoryInfo
		serializer go_serializer.Serializer
	}
)

func NewBadgerOperator(
	manager manager.Manager,
	serializer go_serializer.Serializer,
) Operator {
	o := &BadgerOperator{
		manager:    manager,
		serializer: serializer,
	}

	return o
}

// Operate operates in the given database.
func (o *BadgerOperator) Operate(dbInfo *management_models.DBMemoryInfo) Operator {
	no := &BadgerOperator{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// Create checks if the key exists, if not, it stores the item.
func (o *BadgerOperator) Create(key, value []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			err = txn.Set(key, value)
		}

		return err
	})

	return err
}

// Update updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperator) Update(key, value []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)

		return err
	})

	return err
}

// Load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperator) Load(key []byte) ([]byte, error) {
	var dst []byte
	if err := o.dbInfo.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		dst, err = item.ValueCopy(nil)

		return err
	}); err != nil {
		return dst, err
	}

	return dst, nil
}

// Delete deletes the given key entry if present.
func (o *BadgerOperator) Delete(key []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	return err
}

func (o *BadgerOperator) ListAll() (operation_models.OpList, error) {
	var objectList operation_models.OpList
	err := o.dbInfo.DB.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{}

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			object := &operation_models.OpItem{
				Key:   key,
				Value: value,
			}

			objectList = append(objectList, object)
		}

		return nil
	})

	return objectList, err
}

func (o *BadgerOperator) ListPaginated(
	pagination *management_models.PaginationRequest,
) (operation_models.OpList, error) {
	size := int(pagination.PageSize)
	page := int(pagination.PageID)

	objectList := make(operation_models.OpList, size)
	err := o.dbInfo.DB.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{}
		opts.PrefetchSize = size * page

		it := txn.NewIterator(opts)
		defer it.Close()

		it = o.manager.Paginate(it, size, page)
		for idx := 0; idx < size; idx++ {
			if !it.Valid() {
				break
			}

			item := it.Item()

			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			object := &operation_models.OpItem{
				Key:   key,
				Value: value,
			}

			objectList[idx] = object

			it.Next()
		}

		return nil
	})

	return objectList, err
}
