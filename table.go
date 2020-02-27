package client

import "time"

type FluxTableMetadata struct {
	index   int
	columns []*FluxColumn
}

type FluxColumn struct {
	index        int
	name         string
	dataType     string
	group        bool
	defaultValue string
}

func (f *FluxColumn) SetDefaultValue(defaultValue string) {
	f.defaultValue = defaultValue
}

func (f *FluxColumn) SetGroup(group bool) {
	f.group = group
}

func (f *FluxColumn) SetDataType(dataType string) {
	f.dataType = dataType
}

func (f *FluxColumn) SetName(name string) {
	f.name = name
}

type FluxRecord struct {
	tableIndex int
	values     map[string]interface{}
}

func newFluxTableMetadata(index int) *FluxTableMetadata {
	return &FluxTableMetadata{index: index, columns: make([]*FluxColumn, 0, 10)}
}

func (f *FluxTableMetadata) Index() int {
	return f.index
}

func (f *FluxTableMetadata) Columns() []*FluxColumn {
	return f.columns
}

func (f *FluxTableMetadata) AddColumn(column *FluxColumn) *FluxTableMetadata {
	f.columns = append(f.columns, column)
	return f
}

func (f *FluxTableMetadata) Column(index int) *FluxColumn {
	if len(f.columns) == 0 || index < 0 || index >= len(f.columns) {
		return nil
	}
	return f.columns[index]
}

func newFluxColumn(index int, dataType string) *FluxColumn {
	return &FluxColumn{index: index, dataType: dataType}
}

func (f *FluxColumn) DefaultValue() string {
	return f.defaultValue
}

func (f *FluxColumn) IsGroup() bool {
	return f.group
}

func (f *FluxColumn) DataType() string {
	return f.dataType
}

func (f *FluxColumn) Name() string {
	return f.name
}

func (f *FluxColumn) Index() int {
	return f.index
}
func (r *FluxRecord) TableIndex() int {
	return r.tableIndex
}

func newFluxRecord(table int, values map[string]interface{}) *FluxRecord {
	return &FluxRecord{tableIndex: table, values: values}
}

// Start returns the inclusive lower time bound of all records in the current tableIndex
func (r *FluxRecord) Start() time.Time {
	return r.values["_start"].(time.Time)
}

// Stop returns the exclusive upper time bound of all records in the current tableIndex
func (r *FluxRecord) Stop() time.Time {
	return r.values["_stop"].(time.Time)
}

// Start returns the inclusive lower time bound of all records in the current tableIndex
func (r *FluxRecord) Time() time.Time {
	return r.values["_time"].(time.Time)
}

// Value returns the actual field value
func (r *FluxRecord) Value() interface{} {
	return r.values["_value"]
}

// Stop returns the exclusive upper time bound of all records in the current tableIndex
func (r *FluxRecord) Field() string {
	return r.values["_field"].(string)
}

func (r *FluxRecord) Measurement() string {
	return r.values["_measurement"].(string)
}

func (r *FluxRecord) Values() map[string]interface{} {
	return r.values
}

func (r *FluxRecord) ValueByKey(key string) interface{} {
	return r.values[key]
}
