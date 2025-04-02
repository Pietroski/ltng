package queuemodels

import (
	"fmt"
	"time"

	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

type QueueFanOutType int32

const (
	QueueFanOutTypeQueueFanOutTypeRoundRobin      QueueFanOutType = 0
	QueueFanOutTypeQueueFanOutTypePropagate       QueueFanOutType = 1
	QueueFanOutTypeQueueFanOUtTypeGroupRoundRobin QueueFanOutType = 2
	QueueFanOutTypeQueueFanOUtTypeGroupPropagate  QueueFanOutType = 3
)

// Enum value maps for QueueFanOutType.
var (
	QueueFanOutTypeName = map[int32]string{
		0: "QUEUE_FAN_OUT_TYPE_ROUND_ROBIN",
		1: "QUEUE_FAN_OUT_TYPE_PROPAGATE",
		2: "QUEUE_FAN_OUT_TYPE_GROUP_ROUND_ROBIN",
		3: "QUEUE_FAN_OUT_TYPE_GROUP_PROPAGATE",
	}
	QueueFanOutTypeValue = map[string]int32{
		"QUEUE_FAN_OUT_TYPE_ROUND_ROBIN":       0,
		"QUEUE_FAN_OUT_TYPE_PROPAGATE":         1,
		"QUEUE_FAN_OUT_TYPE_GROUP_ROUND_ROBIN": 2,
		"QUEUE_FAN_OUT_TYPE_GROUP_PROPAGATE":   3,
	}
)

type Group struct {
	Name string
}

type Queue struct {
	Name            string
	Path            string
	QueueFanOutType QueueFanOutType
	CreatedAt       time.Time
	LastStartedAt   time.Time
	Group           *Group
}

type EventMetadata struct {
	Metadata       []byte
	RetryCount     uint64
	SentAt         time.Time
	ReceivedAt     time.Time
	ReceivedAtList []time.Time
}

type Event struct {
	EventID  string
	Queue    *Queue
	Data     []byte
	Metadata *EventMetadata
}

func (e *Event) Validate() error {
	if e == nil {
		return fmt.Errorf("invalid event: nil event")
	}
	if err := e.Metadata.Validate(); err != nil {
		return fmt.Errorf("invalid event metadata: nil metadata")
	}
	if err := e.Queue.Validate(); err != nil {
		return fmt.Errorf("invalid event queue: nil queue")
	}

	return nil
}

func (e *EventMetadata) Validate() error {
	if e == nil {
		return fmt.Errorf("nil event")
	}

	return nil
}

func (q *Queue) Validate() error {
	if q == nil {
		return fmt.Errorf("nil queue")
	}

	return nil
}

func (q *Queue) GetGroupName() string {
	if q.Group == nil {
		return q.Name
	}

	strKey := q.Name + "/" + q.Group.Name
	return strKey
}

func (q *Queue) GetLockKey() string {
	strKey := q.Path + "/" + q.Name
	return strKey
}

func (q *Queue) GetCompleteLockKey() string {
	strKey := q.GetLockKey()
	if q.Group != nil {
		strKey = strKey + "/" + q.Group.Name
	}

	return strKey
}

const (
	QueueNameStore = "ltng_queue_store"
	QueuePathStore = "ltng_queue/queue_store"
)

type QueueOrchestrator struct {
	Queue       *Queue
	PublishList *safe.TicketStorageLoop[*Publisher]
}

type Publisher struct {
	NodeID string
	Sender chan *Event
}
