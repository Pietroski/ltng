package models_badgerdb_v4_operation

type (
	Item struct {
		Key   []byte
		Value []byte
		Error error
	}

	Items []*Item

	IndexOpts struct {
		HasIdx          bool
		ParentKey       []byte
		IndexingKeys    [][]byte
		IndexProperties IndexProperties
	}

	IndexProperties struct {
		IndexDeletionBehaviour IndexDeletionBehaviour
		IndexSearchPattern     IndexSearchPattern
		ListSearchPattern      ListSearchPattern
	}

	CreateOpts struct {
		IndexOpts IndexOpts
	}
)

type IndexDeletionBehaviour int

const (
	None IndexDeletionBehaviour = iota
	Cascade
	IndexOnly
	CascadeByIdx
)

type IndexSearchPattern int

const (
	One IndexSearchPattern = iota
	AndComputational
	OrComputational
	IndexingList
)

type ListSearchPattern int

const (
	Default ListSearchPattern = iota
	All
)
