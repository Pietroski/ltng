package safe

import (
	"testing"
)

func TestTicketStorageLoop(t *testing.T) {
	t.Run("", func(t *testing.T) {
		tsl := NewTicketStorageLoop[int](WithTicketStorageSize[int](2))
		limit := 10

		tsl.Put(1)
		tsl.Put(2)

		t.Log(len(tsl.Get()), tsl.Get())

		for i := 0; i < limit; i++ {
			t.Log(tsl.Next())
		}
	})

	t.Run("", func(t *testing.T) {
		tsl := NewTicketStorageLoop[int](WithTicketStorageSize[int](3))
		limit := 10

		tsl.Put(1)
		tsl.Put(2)
		tsl.Put(3)

		t.Log(len(tsl.Get()), tsl.Get())

		for i := 0; i < limit; i++ {
			t.Log(tsl.Next())
		}
	})
}
