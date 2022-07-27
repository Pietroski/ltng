package mock_generator

import _ "github.com/golang/mock/mockgen/model"

//go:generate mockgen -package mock_manager -destination ../../internal/adaptors/datastore/badgerdb/manager/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager Manager
//go:generate mockgen -package mock_indexed_manager -destination ../../internal/adaptors/datastore/badgerdb/manager/indexed/mocks/indexed_manager.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed IndexerManager

//go:generate mockgen -package mock_operator -destination ../../internal/adaptors/datastore/badgerdb/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations Operator
//go:generate mockgen -package mock_indexed_operator -destination ../../internal/adaptors/datastore/badgerdb/transactions/operations/indexed/mocks/indexed_operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations/indexed IndexedOperator
