package safe

import (
	"strconv"
	"sync"
	"testing"
)

// Helper function to populate map with test data
func populateMap[T any](m *GenericMap[T], size int, getValue func(i int) T) {
	for i := 0; i < size; i++ {
		m.Set("key"+strconv.Itoa(i), getValue(i))
	}
}

func BenchmarkRangeMethods(b *testing.B) {
	// Test different map sizes
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run("Size_"+strconv.Itoa(size), func(b *testing.B) {
			// Test callback Range method
			b.Run("Range_Callback", func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					m.Range(func(key string, value int) bool {
						count += value
						return true
					})
				}
			})

			// Test Entries method
			b.Run("Entries_Map", func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					entries := m.Entries()
					for _, v := range entries {
						count += v
					}
				}
			})

			// Test Channel Iterator method
			b.Run("Channel_Iterator", func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					for kv := range m.Iterator() {
						count += kv.Value
					}
				}
			})
		})
	}
}

// Benchmark concurrent access patterns
func BenchmarkConcurrentRange(b *testing.B) {
	sizes := []int{1000}
	goroutines := []int{1, 4, 8, 16}

	for _, size := range sizes {
		for _, numG := range goroutines {
			name := "Size_" + strconv.Itoa(size) + "_Goroutines_" + strconv.Itoa(numG)

			b.Run("Concurrent_Range_"+name, func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						m.Range(func(key string, value int) bool {
							return true
						})
					}
				})
			})

			b.Run("Concurrent_Entries_"+name, func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						entries := m.Entries()
						for range entries {
							// Just iterate
						}
					}
				})
			})

			b.Run("Concurrent_Iterator_"+name, func(b *testing.B) {
				m := NewGenericMap[int]()
				populateMap(m, size, func(i int) int { return i })

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for range m.Iterator() {
							// Just iterate
						}
					}
				})
			})
		}
	}
}

// Benchmark with mixed operations (range + modifications)
func BenchmarkMixedOperations(b *testing.B) {
	m := NewGenericMap[int]()
	populateMap(m, 1000, func(i int) int { return i })

	b.Run("Mixed_Range_WithWrites", func(b *testing.B) {
		var wg sync.WaitGroup
		b.ResetTimer()

		// Start writer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				m.Set("new_key"+strconv.Itoa(i), i)
				if i%100 == 0 {
					m.Delete("new_key" + strconv.Itoa(i-100))
				}
			}
		}()

		// Reader goroutines using different range methods
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(method int) {
				defer wg.Done()
				for i := 0; i < b.N; i++ {
					switch method {
					case 0:
						m.Range(func(key string, value int) bool {
							return true
						})
					case 1:
						entries := m.Entries()
						for range entries {
						}
					case 2:
						for range m.Iterator() {
						}
					}
				}
			}(i)
		}

		wg.Wait()
	})
}
