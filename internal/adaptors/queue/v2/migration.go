package ltngqueueenginev2

import (
	"context"

	ltngdbmodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	queuemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/queue"
)

func (q *Queue) runQueueMigration(ctx context.Context) (*ltngdbmodelsv3.StoreInfo, error) {
	info := &ltngdbmodelsv3.StoreInfo{
		Name: queuemodels.QueueNameStore,
		Path: queuemodels.QueuePathStore,
	}
	queueStore, err := q.ltngdbengine.CreateStore(ctx, info)
	if err != nil {
		return nil, err
	}

	return queueStore, nil
}
