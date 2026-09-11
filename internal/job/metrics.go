package job

type Metrics struct {
	Total         int64 `json:"total"`
	Queued        int64 `json:"queued"`
	Running       int64 `json:"running"`
	Retrying      int64 `json:"retrying"`
	Completed     int64 `json:"completed"`
	Failed        int64 `json:"failed"`
	PendingOutbox int64 `json:"pending_outbox"`
}
