package ltng_engine_models

type (
	DBInfo struct {
		Name         string `json:"name,omitempty"`
		Path         string `json:"path,omitempty"`
		CreatedAt    int64  `json:"createdAt"`
		LastOpenedAt int64  `json:"lastOpenedAt"`
	}

	CreateStore struct {
		Name string `json:"name,omitempty"`
		Path string `json:"path,omitempty"`
	}

	Pagination struct {
		PageID   uint64 `json:"page_id,omitempty"`
		PageSize uint64 `json:"page_size,omitempty"`
	}
)

func (p *Pagination) IsValid() bool {
	if p != nil && p.PageID > 0 && p.PageSize > 0 {
		return true
	}

	return false
}
