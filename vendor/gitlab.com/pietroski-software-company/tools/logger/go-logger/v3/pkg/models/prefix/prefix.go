package prefix_models

type Prefix string

const (
	ErrPrefix   Prefix = "[ERROR]: "
	WarnPrefix  Prefix = "[WARN]: "
	InfoPrefix  Prefix = "[INFO]: "
	DebugPrefix Prefix = "[DEBUG]: "
)

func (e Prefix) String() string {
	return string(e)
}
