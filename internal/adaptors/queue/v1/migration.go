package ltngqueue_engine

import (
	"context"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
)

func (q *Queue) runQueueMigration(ctx context.Context) (*ltngenginemodels.StoreInfo, error) {
	info := &ltngenginemodels.StoreInfo{
		Name: queuemodels.QueueNameStore,
		Path: queuemodels.QueuePathStore,
	}
	queueStore, err := q.ltngdbengine.CreateStore(ctx, info)
	if err != nil {
		return nil, err
	}

	return queueStore, nil
}
