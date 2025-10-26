package mock_generator

import _ "go.uber.org/mock/mockgen/model"

// adaptors
// // datastore
// // // badgerdb
// // // // v4
//go:generate mockgen -package mocks -destination ../../internal/adaptors/datastore/badgerdb/v4/mocks/manager.go gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4 Manager
//go:generate mockgen -package mocks -destination ../../internal/adaptors/datastore/badgerdb/v4/mocks/operations.go gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4 Operator

// client
//go:generate mockgen -package mocked_ltng_client -destination ../../client/mocks/ltng_client_mock.go gitlab.com/pietroski-software-company/lightning-db/client Client
