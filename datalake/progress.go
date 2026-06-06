package datalake

type Phase string

const (
	PhaseValidating Phase = "validating"
	PhaseParsing    Phase = "parsing"
	PhaseUpserting  Phase = "upserting"
	PhaseMoving     Phase = "moving"
	PhaseDone       Phase = "done"
	PhaseFailed     Phase = "failed"
)

type ProgressEvent struct {
	Phase          Phase  `json:"phase"`
	Message        string `json:"message,omitempty"`
	FileName       string `json:"fileName,omitempty"`
	CurrentRecord  int64  `json:"currentRecord,omitempty"`
	TotalRecords   int64  `json:"totalRecords,omitempty"`
	UpsertedCount  int64  `json:"upsertedCount,omitempty"`
	DuplicateCount int64  `json:"duplicateCount,omitempty"`
}

type ProgressReporter interface {
	Report(ProgressEvent)
}
