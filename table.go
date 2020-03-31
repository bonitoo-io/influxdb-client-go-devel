package client

import "time"

// FluxTableMetadata holds flux query result table information represented by collection of columns.
// Each new table is introduced by annotations
type FluxTableMetadata struct {
	position int
	columns  []*FluxColumn
}

// FluxColumn holds flux query table column properties
type FluxColumn struct {
	index        int
	name         string
	dataType     string
	group        bool
	defaultValue string
}

// FluxRecord represents row in the flux query result table
type FluxRecord struct {
	table  int
	values map[string]interface{}
}

// newFluxTableMetadata creates FluxTableMetadata for the table on position
func newFluxTableMetadata(position int) *FluxTableMetadata {
	return &FluxTableMetadata{position: position, columns: make([]*FluxColumn, 0, 10)}
}

// Position returns position of the table in the flux query result
func (f *FluxTableMetadata) Position() int {
	return f.position
}

// Columns returns slice of flux query result table
func (f *FluxTableMetadata) Columns() []*FluxColumn {
	return f.columns
}

// AddColumn adds column definition to table metadata
func (f *FluxTableMetadata) AddColumn(column *FluxColumn) *FluxTableMetadata {
	f.columns = append(f.columns, column)
	return f
}

// Column returns flux table column by index
// Returns nil if index is out of the bounds
func (f *FluxTableMetadata) Column(index int) *FluxColumn {
	if len(f.columns) == 0 || index < 0 || index >= len(f.columns) {
		return nil
	}
	return f.columns[index]
}

// SetDefaultValue sets default value for the column
func (f *FluxColumn) SetDefaultValue(defaultValue string) {
	f.defaultValue = defaultValue
}

// SetGroup set group flag for the column
func (f *FluxColumn) SetGroup(group bool) {
	f.group = group
}

// SetDataType sets data type for the column
func (f *FluxColumn) SetDataType(dataType string) {
	f.dataType = dataType
}

// SetName sets name of the column
func (f *FluxColumn) SetName(name string) {
	f.name = name
}

// newFluxColumn creates FluxColumn for position and data type
func newFluxColumn(index int, dataType string) *FluxColumn {
	return &FluxColumn{index: index, dataType: dataType}
}

// DefaultValue returns default value of the column
func (f *FluxColumn) DefaultValue() string {
	return f.defaultValue
}

// IsGroup return true if the column is grouping column
func (f *FluxColumn) IsGroup() bool {
	return f.group
}

// DataType returns data type of the column
func (f *FluxColumn) DataType() string {
	return f.dataType
}

// Name returns name of the column
func (f *FluxColumn) Name() string {
	return f.name
}

// Index returns index of the column
func (f *FluxColumn) Index() int {
	return f.index
}

// Table returns index of the table record belongs to
func (r *FluxRecord) Table() int {
	return r.table
}

// newFluxRecord returns new record for the table with values
func newFluxRecord(table int, values map[string]interface{}) *FluxRecord {
	return &FluxRecord{table: table, values: values}
}

// Start returns the inclusive lower time bound of all records in the current table
func (r *FluxRecord) Start() time.Time {
	return r.ValueByKey("_start").(time.Time)
}

// Stop returns the exclusive upper time bound of all records in the current table
func (r *FluxRecord) Stop() time.Time {
	return r.ValueByKey("_stop").(time.Time)
}

// Start returns the time of the record
func (r *FluxRecord) Time() time.Time {
	return r.ValueByKey("_time").(time.Time)
}

// Value returns the actual field value
func (r *FluxRecord) Value() interface{} {
	return r.ValueByKey("_value")
}

// Field returns the field name
func (r *FluxRecord) Field() string {
	return r.ValueByKey("_field").(string)
}

// Measurement returns the measurement name of the record
func (r *FluxRecord) Measurement() string {
	return r.ValueByKey("_measurement").(string)
}

// Values returns map of the values where key is the column name
func (r *FluxRecord) Values() map[string]interface{} {
	return r.values
}

// ValueByKey returns value for given column key for the record
func (r *FluxRecord) ValueByKey(key string) interface{} {
	return r.values[key]
}
