package operation_models

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
		//IndexCreationBehaviour IndexCreationBehaviour
		//IndexUpsertBehaviour   IndexUpsertBehaviour
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
