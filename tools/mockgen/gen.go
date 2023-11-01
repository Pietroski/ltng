package mock_generator

import _ "github.com/golang/mock/mockgen/model"

//go:generate mockgen -package mock_manager -destination ../../internal/adaptors/datastore/badgerdb/v3/manager/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager Manager
//go:generate mockgen -package mock_operations -destination ../../internal/adaptors/datastore/badgerdb/v3/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations Operator

///go:generate mockgen -package mock_operator -destination ../../internal/adaptors/datastore/badgerdb/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations Operator
//go:generate mockgen -package chained_mock -destination ../../pkg/tools/chained-operator/mocks/chained_mock.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator/mocks Callers

// pkg
//go:generate mockgen -package mocked_ltng_client -destination ../../pkg/client/mocks/ltng_client_mock.go gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/client LTNGClient
