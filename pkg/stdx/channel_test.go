package stdx

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestExample struct {
	Int  int
	Str  string
	Bool bool
}

func TestChannel(t *testing.T) {
	t.Run("IsClosed Single Thread", func(t *testing.T) {
		ch := NewChannel[*TestExample](
			WithChannelSize[*TestExample](1),
		)

		isClosed := ch.IsClosed()
		assert.False(t, isClosed)

		ch.Ch <- &TestExample{
			Int:  1,
			Str:  "String",
			Bool: true,
		}

		testExample := <-ch.Ch
		assert.Equal(t, 1, testExample.Int)
		assert.Equal(t, "String", testExample.Str)
		assert.Equal(t, true, testExample.Bool)

		ch.Close()
		isClosed = ch.IsClosed()
		assert.True(t, isClosed)

		_, ok := <-ch.Ch
		assert.False(t, ok)
	})

	t.Run("IsClosed Multi Thread", func(t *testing.T) {
		ch := NewChannel[*TestExample](
			WithChannelSize[*TestExample](10),
		)

		isClosed := ch.IsClosed()
		assert.False(t, isClosed)

		limit := 10
		wg := sync.WaitGroup{}
		for i := 0; i < limit; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch.Ch <- &TestExample{
					Int:  i,
					Str:  fmt.Sprintf("string %d", i),
					Bool: i%2 == 0,
				}
			}()
		}
		wg.Wait()

		for i := 0; i < limit; i++ {
			testExample := <-ch.Ch
			//assert.Contains(t, i, testExample.Int)
			t.Logf("testExample: %+v", testExample)
			assert.Equal(t, fmt.Sprintf("string %d", testExample.Int), testExample.Str)
			assert.Equal(t, testExample.Int%2 == 0, testExample.Bool)
		}

		wg = sync.WaitGroup{}
		for i := 0; i < limit; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := ch.Send(&TestExample{
					Int:  i,
					Str:  fmt.Sprintf("string %d", i),
					Bool: i%2 == 0,
				})
				assert.True(t, ok)
			}()
		}
		wg.Wait()

		wg = sync.WaitGroup{}
		for i := 0; i < limit; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch.Close()
				isClosed := ch.IsClosed()
				assert.True(t, isClosed)
			}()
		}
		wg.Wait()

		wg = sync.WaitGroup{}
		for i := 0; i < limit; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := ch.Send(&TestExample{
					Int:  i,
					Str:  fmt.Sprintf("string %d", i),
					Bool: i%2 == 0,
				})
				assert.False(t, ok)
			}()
		}
		wg.Wait()

		ch.Close()
		isClosed = ch.IsClosed()
		assert.True(t, isClosed)

		for i := 0; i < limit; i++ {
			testExample := <-ch.Ch
			//assert.Contains(t, i, testExample.Int)
			t.Logf("testExample: %+v", testExample)
			assert.Equal(t, fmt.Sprintf("string %d", testExample.Int), testExample.Str)
			assert.Equal(t, testExample.Int%2 == 0, testExample.Bool)
		}
	})
}
