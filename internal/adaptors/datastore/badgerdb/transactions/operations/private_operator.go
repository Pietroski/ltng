package operations

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
)

// create checks if the key exists, if not, it stores the item.
func (o *BadgerOperator) create(key, value []byte) error {
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
func (o *BadgerOperator) createWithTxn(txn *badger.Txn, key, value []byte) error {
	_, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		err = txn.Set(key, value)
	} else if err == nil {
		return fmt.Errorf(ErrKeyAlreadyExist)
	}

	return err
}

// upsert updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperator) upsert(key, value []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)

		return err
	})

	return err
}

// upsertWithTxn updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperator) upsertWithTxn(txn *badger.Txn, key, value []byte) error {
	return txn.Set(key, value)
}

// load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperator) load(key []byte) ([]byte, error) {
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
func (o *BadgerOperator) delete(key []byte) error {
	err := o.dbInfo.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	return err
}

// listAll lists all entries from the database info set on the Operate method.
func (o *BadgerOperator) listAll() (operation_models.Items, error) {
	var objectList operation_models.Items
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

			object := &operation_models.Item{
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
func (o *BadgerOperator) listPaginated(
	pagination *management_models.Pagination,
) (operation_models.Items, error) {
	size := int(pagination.PageSize)
	page := int(pagination.PageID)

	objectList := make(operation_models.Items, size)
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

			object := &operation_models.Item{
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
