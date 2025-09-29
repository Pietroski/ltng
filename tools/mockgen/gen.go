package mock_generator

import _ "go.uber.org/mock/mockgen/model"

// adaptors
// // datastore
// // // badgerdb
// // // // v4
//go:generate mockgen -package mocks -destination ../../internal/adaptors/datastore/badgerdb/v4/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4 Manager
//go:generate mockgen -package mocks -destination ../../internal/adaptors/datastore/badgerdb/v4/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4 Operator

///go:generate mockgen -package mock_operator -destination ../../internal/adaptors/datastore/badgerdb/transactions/operations/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/transactions/operations Operator
//go:generate mockgen -package chained_mock -destination ../../pkg/tools/chained-operator/mocks/chained_mock.go gitlab.com/pietroski-software-company/lightning-db/pkg/tools/chained-operator/mocks Callers

// client
//go:generate mockgen -package mocked_ltng_client -destination ../../client/mocks/ltng_client_mock.go gitlab.com/pietroski-software-company/lightning-db/client Client
