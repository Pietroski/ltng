package queuemodels

import (
	"fmt"
	"os"
	"sync/atomic"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

type QueueDistributionType int32

const (
	QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN QueueDistributionType = 0
	QueueDistributionType_QUEUE_DISTRIBUTION_TYPE_FAN_OUT     QueueDistributionType = 1
)

// Enum value maps for QueueDistributionType.
var (
	QueueDistributionType_name = map[int32]string{
		0: "QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN",
		1: "QUEUE_DISTRIBUTION_TYPE_FAN_OUT",
	}
	QueueDistributionType_value = map[string]int32{
		"QUEUE_DISTRIBUTION_TYPE_ROUND_ROBIN": 0,
		"QUEUE_DISTRIBUTION_TYPE_FAN_OUT":     1,
	}
)

type ACKPolicy int32

const (
	ACKPolicy_ACK_POLICY_AT_LEAST_ONE ACKPolicy = 0
	ACKPolicy_ACK_POLICY_ALL          ACKPolicy = 1
)

// Enum value maps for ACKPolicy.
var (
	ACKPolicy_name = map[int32]string{
		0: "ACKPolicy_ACK_POLICY_AT_LEAST_ONE",
		1: "ACKPolicy_ACK_POLICY_ALL",
	}
	ACKPolicy_value = map[string]int32{
		"ACKPolicy_ACK_POLICY_AT_LEAST_ONE": 0,
		"ACKPolicy_ACK_POLICY_ALL":          1,
	}
)

type Group struct {
	Name string
}

type Queue struct {
	Name          string
	Path          string
	MaxRetries    uint64
	MaxRetryDelay uint64

	// Group is a valid field for subscribers and publishers
	Group *Group
	// QueueDistributionType is a valid field for subscribers.
	QueueDistributionType QueueDistributionType
	// At least one or at lest all subscribers should ack the message.
	AckPolicy ACKPolicy

	CreatedAt     int64 // time.Time
	LastStartedAt int64 // time.Time
}

type EventMetadata struct {
	Metadata       []byte
	RetryCount     uint64
	SentAt         int64   // time.Time
	ReceivedAt     int64   // time.Time
	ReceivedAtList []int64 // []time.Time
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

func (emd *EventMetadata) Validate() error {
	if emd == nil {
		return fmt.Errorf("nil event metadata")
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

	strKey := q.Name + "|" + q.Group.Name
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

	Publishers    = "publishers"
	Signalers     = "signalers"
	Sep           = string(os.PathSeparator)
	PublishersSep = Publishers + Sep
	SignalersSep  = Signalers + Sep
)

type QueueOrchestrator struct {
	Queue       *Queue
	PublishList *safe.TicketStorageLoop[*Publisher]
}

type Publisher struct {
	NodeID string
	Sender chan *Event
}

type EventTracker struct {
	EventID string
	Event   *Event
	Ack     chan struct{}
	Nack    chan struct{}

	WasACKed  *atomic.Bool
	WasNACKed *atomic.Bool
}

type QueuePublisher struct {
	FileQueue *filequeuev1.FileQueue

	FirstSent *atomic.Bool
	IsClosed  *atomic.Bool
}

type QueueSignaler struct {
	FileQueue         *filequeuev1.FileQueue
	SignalTransmitter chan struct{}
	FirstSent         *atomic.Bool
	IsClosed          *atomic.Bool
}
