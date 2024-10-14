package common_model

import "strings"

type EngineVersionType string

const (
	DefaultEngineVersionType    EngineVersionType = "DEFAULT"
	BadgerDBV3EngineVersionType EngineVersionType = "BADGER-DB-ENGINE_V3"
	BadgerDBV4EngineVersionType EngineVersionType = "BADGER-DB-ENGINE_V4"
)

func ToEngineVersionType(e string) EngineVersionType {
	return EngineVersionType(strings.ToUpper(e))
}

func (e EngineVersionType) String() string {
	return string(e)
}

func (e EngineVersionType) ToLower() string {
	return strings.ToLower(e.String())
}
