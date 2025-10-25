package validator

type (
	checker     func(fieldName string, fieldValue any, parameter string) error
	mapChecker  map[string]checker
	listChecker []checker
)

var listCheckers = listChecker{
	checkRequirement,

	minLength,
	maxLength,
	eqLength,

	checkType,

	checkAfterTime,
	checkBeforeTime,
}

var checkers = mapChecker{
	"required": checkRequirement,

	"min": minLength,
	"max": maxLength,
	"len": eqLength,

	"type": checkType,

	"after":  checkAfterTime,
	"before": checkBeforeTime,

	"uuid":        uuidValidator,
	"string_uuid": stringUUIDValidator,
}
