package safe

import "sync"

type GenericMap[T any] struct {
	sync.RWMutex
	data map[string]T
}

func NewGenericMap[T any]() *GenericMap[T] {
	gm := &GenericMap[T]{
		data: make(map[string]T),
	}

	return gm
}

func (m *GenericMap[T]) Set(key string, value T) {
	m.Lock()
	m.data[key] = value
	m.Unlock()
}

func (m *GenericMap[T]) Get(key string) (T, bool) {
	m.RLock()
	value, exists := m.data[key]
	m.RUnlock()
	return value, exists
}

func (m *GenericMap[_]) Delete(key string) {
	m.Lock()
	delete(m.data, key)
	m.Unlock()
}

func (m *GenericMap[_]) Size() int {
	m.RLock()
	size := len(m.data)
	m.RUnlock()
	return size
}

// Range ranges over the generic map concurrently safe.
// if false is returned, it breaks the loop
func (m *GenericMap[T]) Range(fn func(key string, value T) bool) {
	m.RLock()
	defer m.RUnlock()

	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}

func (m *GenericMap[T]) RangeAndDelete(condition func(key string, value T) bool) {
	// First collect keys to delete
	var toDelete []string
	m.Range(func(key string, value T) bool {
		if condition(key, value) {
			toDelete = append(toDelete, key)
		}

		return true
	})

	// Then delete them
	for _, key := range toDelete {
		m.Delete(key)
	}
}

func (m *GenericMap[T]) Entries() map[string]T {
	m.RLock()
	defer m.RUnlock()

	result := make(map[string]T, len(m.data))
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *GenericMap[T]) Iterator() <-chan KeyValue[T] {
	ch := make(chan KeyValue[T])

	go func() {
		m.RLock()
		defer m.RUnlock()

		for k, v := range m.data {
			ch <- KeyValue[T]{Key: k, Value: v}
		}
		close(ch)
	}()

	return ch
}

type KeyValue[T any] struct {
	Key   string
	Value T
}
