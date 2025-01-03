package ltngenginemodels

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPagination_CalcNextPage(t *testing.T) {
	testCases := map[string]struct {
		currentPagination *Pagination
		nextPagination    *Pagination
		itemLength        uint64
	}{
		"odd": {
			currentPagination: &Pagination{
				PageID:   1,
				PageSize: 5,
			},
			nextPagination: &Pagination{
				PageID:   2,
				PageSize: 5,
			},
			itemLength: 11,
		},
		"odd-next": {
			currentPagination: &Pagination{
				PageID:   2,
				PageSize: 5,
			},
			nextPagination: &Pagination{
				PageID:   3,
				PageSize: 1,
			},
			itemLength: 11,
		},
		"even": {
			currentPagination: &Pagination{
				PageID:   1,
				PageSize: 5,
			},
			nextPagination: &Pagination{
				PageID:   2,
				PageSize: 5,
			},
			itemLength: 10,
		},
		"even-next": {
			currentPagination: &Pagination{
				PageID:   2,
				PageSize: 5,
			},
			nextPagination: &Pagination{
				PageID:   3,
				PageSize: 0,
			},
			itemLength: 10,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t,
				tc.nextPagination,
				tc.currentPagination.CalcNextPage(tc.itemLength),
			)
			assert.True(t,
				tc.currentPagination.
					CalcNextPage(tc.itemLength).
					IsValid())
		})
	}
}
