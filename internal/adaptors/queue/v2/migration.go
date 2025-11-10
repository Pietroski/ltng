package ltngqueue_engine

import (
	"context"

	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (q *Queue) runQueueMigration(ctx context.Context) (*ltngdata.StoreInfo, error) {
	info := &ltngdata.StoreInfo{
		Name: queuemodels.QueueNameStore,
		Path: queuemodels.QueuePathStore,
	}
	queueStore, err := q.ltngdbengine.CreateStore(ctx, info)
	if err != nil {
		return nil, err
	}

	return queueStore, nil
}
