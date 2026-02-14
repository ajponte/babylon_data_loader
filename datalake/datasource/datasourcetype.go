package datasource

// DataSource represents the type of data source.
type DataSource string

const (
	// Generic represents a generic data source.
	Generic DataSource = "generic"
	// Chase represents the Chase data source.
	Chase DataSource = "chase"
	// Synthetic represents a synthetic data source.
	Synthetic DataSource = "synthetic"
)
