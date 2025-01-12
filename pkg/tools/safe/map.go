package safe

import "sync"

type Map struct {
	sync.RWMutex
	data map[string]interface{}
}

func NewMap() *Map {
	return &Map{
		data: make(map[string]interface{}),
	}
}

func (m *Map) Set(key string, value interface{}) {
	m.Lock()
	m.data[key] = value
	m.Unlock()
}

func (m *Map) Get(key string) (interface{}, bool) {
	m.RLock()
	value, exists := m.data[key]
	m.RUnlock()
	return value, exists
}

func (m *Map) Delete(key string) {
	m.Lock()
	delete(m.data, key)
	m.Unlock()
}

func (m *Map) Size() int {
	m.RLock()
	size := len(m.data)
	m.RUnlock()
	return size
}

// Range ranges over the generic map concurrently safe.
// if false is returned, it breaks the loop
func (m *Map) Range(fn func(key string, value any) bool) {
	m.RLock()
	defer m.RUnlock()

	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}
