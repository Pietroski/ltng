package safe

import (
	"testing"

	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

func TestTicketStorage(t *testing.T) {
	t.Run("", func(t *testing.T) {
		ts := NewTicketStorage[int](10)
		limit := 10

		op := syncx.NewThreadOperator("ticket_storage")
		for i := 0; i < limit; i++ {
			op.Op(func() {
				ts.Put(i)
			})
		}
		op.Wait()

		t.Log(ts.ticket.Load(), ts.done.Load())
		t.Log(len(ts.Get()), ts.Get())
	})

	t.Run("", func(t *testing.T) {
		ts := NewTicketStorage[int](0)
		limit := 10

		op := syncx.NewThreadOperator("ticket_storage")
		for i := 0; i < limit; i++ {
			op.Op(func() {
				ts.Append(i)
			})
		}
		op.Wait()

		t.Log(ts.ticket.Load(), ts.done.Load())
		t.Log(len(ts.Get()), ts.Get())
	})

	t.Run("", func(t *testing.T) {
		ts := NewTicketStorage[int](0)
		limit := 10

		op := syncx.NewThreadOperator("ticket_storage")
		for i := 0; i < limit; i++ {
			op.Op(func() {
				ts.Append(i)
			})
		}
		op.Wait()

		t.Log(ts.ticket.Load(), ts.done.Load())
		t.Log(len(ts.Get()), ts.Get())

		ts.Slice(2, 5)

		t.Log(ts.ticket.Load(), ts.done.Load())
		t.Log(len(ts.Get()), ts.Get())
	})

	t.Run("", func(t *testing.T) {
		ts := NewTicketStorage[int](0)
		limit := 10

		op := syncx.NewThreadOperator("ticket_storage")
		for i := 0; i < limit; i++ {
			op.Op(func() {
				ts.Append(i)
			})
		}
		op.Wait()

		t.Log(ts.ticket.Load(), ts.done.Load())
		t.Log(len(ts.Get()), ts.Get())

		op = syncx.NewThreadOperator("ticket_storage")
		for i := 9; i >= 0; i-- {
			op.Op(func() {
				ts.FindAndDelete(func(item int) bool {
					if item == i {
						return true
					}

					return false
				})
				// t.Log(i, ts.done.Load()) //, len(ts.Get()), ts.Get())
			})
		}
		op.Wait()

		t.Log(ts.ticket.Load(), ts.done.Load(), len(ts.Get()), ts.Get())
	})
}
