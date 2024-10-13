package badgerdb_operations_adaptor_v4

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
)

// create checks if the key exists, if not, it stores the item.
func (o *BadgerOperatorV4) create(key, value []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			err = txn.Set(key, value)
		} else if err == nil {
			return fmt.Errorf(ErrKeyAlreadyExist)
		}

		return err
	})

	return err
}

// createWithTxn checks if the key exists, if not, it stores the item.
func (o *BadgerOperatorV4) createWithTxn(txn *badger.Txn, key, value []byte) error {
	item, err := txn.Get(key)
	_ = item
	if err == badger.ErrKeyNotFound {
		err = txn.Set(key, value)
	} else if err == nil {
		return fmt.Errorf(ErrKeyAlreadyExist)
	}

	return err
}

// upsert updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperatorV4) upsert(key, value []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)

		return err
	})

	return err
}

// upsertWithTxn updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperatorV4) upsertWithTxn(txn *badger.Txn, key, value []byte) error {
	return txn.Set(key, value)
}

// load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperatorV4) load(key []byte) ([]byte, error) {
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

// delete deletes the given key entry if present.
func (o *BadgerOperatorV4) delete(key []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	return err
}

// listAll lists all entries from the database info set on the Operate method.
func (o *BadgerOperatorV4) listAll() (badgerdb_operation_models_v4.Items, error) {
	var objectList badgerdb_operation_models_v4.Items
	err := o.dbInfo.DB.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			PrefetchSize:   500,
			PrefetchValues: true,
		}

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			object := &badgerdb_operation_models_v4.Item{
				Key:   key,
				Value: value,
			}

			objectList = append(objectList, object)
		}

		return nil
	})

	return objectList, err
}

// ListPaginated lists paginated entries from the database info set on the Operate method.
func (o *BadgerOperatorV4) listPaginated(
	pagination *badgerdb_management_models_v4.Pagination,
) (badgerdb_operation_models_v4.Items, error) {
	size := int(pagination.PageSize)
	page := int(pagination.PageID)

	objectList := make(badgerdb_operation_models_v4.Items, size)
	err := o.dbInfo.DB.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			PrefetchSize:   500,
			PrefetchValues: true,
		}
		if size*page > opts.PrefetchSize {
			opts.PrefetchSize = size * page
		}

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

			object := &badgerdb_operation_models_v4.Item{
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
