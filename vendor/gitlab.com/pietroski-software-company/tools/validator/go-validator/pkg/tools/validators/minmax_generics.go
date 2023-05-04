package go_validator

type (
	Number interface {
		uint | uint8 | uint16 | uint32 | uint64 |
			int | int8 | int16 | int32 | int64 |
			float32 | float64
	}

	PtrNumber interface {
		*uint | *uint8 | *uint16 | *uint32 | *uint64 |
			*int | *int8 | *int16 | *int32 | *int64 |
			*float32 | *float64
	}
)

func ltOp[T Number](value, number T) bool {
	return value < number
}

func gtOp[T Number](value, number T) bool {
	return value > number
}

func eqOp[T Number](value, number T) bool {
	return value == number
}

func neOp[T Number](value, number T) bool {
	return value != number
}
