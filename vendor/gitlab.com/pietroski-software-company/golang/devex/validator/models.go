package validator

const (
	NothingToCheckErrMsg = "nothing to check"
	NilObjErrMsg         = "nil object"
	EmptyObjErrMsg       = "empty object"

	ErrorRequiredField    = "error: %v is a mandatory field"
	ErrorShouldNotBeEmpty = "error: %v is a mandatory field and should not be empty"
	ErrorShouldNotBeZero  = "error: %v is a mandatory field and should not be a zero value"
)
