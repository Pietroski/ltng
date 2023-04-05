package go_publisher

type (
	Publisher interface {
		Publish(payload interface{}) error
	}
)
