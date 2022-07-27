package indexed_operation_models

import (
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"
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
)

type (
	IdxOpsIndex struct {
		ShallIndex      bool
		ParentKey       []byte
		IndexKeys       [][]byte
		IndexProperties IndexProperties
	}

	IndexProperties struct {
		IndexDeletionBehaviour IndexDeletionBehaviour
		IndexSearchPattern     IndexSearchPattern
	}
)

func GetItemFromRequest(
	item *grpc_ops.Item,
) *operation_models.OpItem {
	return &operation_models.OpItem{
		Key:   item.GetKey(),
		Value: item.GetValue(),
	}
}

func GetIndexFromRequest(
	index *grpc_indexed_ops.Index,
) *IdxOpsIndex {
	return &IdxOpsIndex{
		ShallIndex: index.GetShallIndex(),
		ParentKey:  index.GetParentKey(),
		IndexKeys:  index.GetIndexKeys(),
		IndexProperties: IndexProperties{
			IndexDeletionBehaviour: IndexDeletionBehaviour(
				index.GetIndexProperties().GetIndexDeletionBehaviour()),
			IndexSearchPattern: IndexSearchPattern(
				index.GetIndexProperties().GetIndexSearchPattern()),
		},
	}
}
