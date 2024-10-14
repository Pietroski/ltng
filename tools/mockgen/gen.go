//go:build tools
// +build tools

package mock_generator

import _ "go.uber.org/mock/mockgen/model"

// adaptors
// // datastore
// // // badgerdb
// // // // v3
//go:generate mockgen -package mock_badgerdb_manager_adaptor_v3 -destination ../../internal/adaptors/datastore/badgerdb/v3/manager/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager Manager
//go:generate mockgen -package mock_badgerdb_operations_adaptor_v3 -destination ../../internal/adaptors/datastore/badgerdb/v3/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations Operator
// // // // v4
//go:generate mockgen -package mock_badgerdb_manager_adaptor_v4 -destination ../../internal/adaptors/datastore/badgerdb/v4/manager/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager Manager
//go:generate mockgen -package mock_badgerdb_operations_adaptor_v4 -destination ../../internal/adaptors/datastore/badgerdb/v4/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/transactions/operations Operator

///go:generate mockgen -package mock_operator -destination ../../internal/adaptors/datastore/badgerdb/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations Operator
//go:generate mockgen -package chained_mock -destination ../../pkg/tools/chained-operator/mocks/chained_mock.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator/mocks Callers

// pkg
//go:generate mockgen -package mocked_ltng_client -destination ../../pkg/client/mocks/ltng_client_mock.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/client LTNGClient
