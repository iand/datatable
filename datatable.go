// Package datatable provides a column-centric data structure for aggregating data
// See https://github.com/Rdatatable/data.table/wiki for inspiration
package datatable

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
)

const yieldThreadPoint = 1000

var ErrInvalidColumnLength = errors.New("invalid column length")
var ErrMismatchedColumnTypes = errors.New("mismatched column types")
var ErrWrongNumberOfColumns = errors.New("wrong number of columns in data")

type colvals struct {
	f []float64
	s []string
}

func (cv colvals) Len() int {
	if cv.f != nil {
		return len(cv.f)
	}
	return len(cv.s)
}

// DataTable is a column-centric table of data. Columns can be either numeric (float64)
// or text (string). A DataTable is not safe for concurrent use.
type DataTable struct {
	cols     []colvals
	colnames []string
	colorder map[string]int
	keys     []int
}

// AddColumn adds a column of float64 data. The length of the column
// must equal the length of any other columns already present in
// the table.
func (dt *DataTable) AddColumn(name string, values []float64) error {
	if len(dt.cols) != 0 && len(values) != dt.Len() {
		return ErrInvalidColumnLength
	}
	dt.addColumn(name, colvals{f: values})
	return nil
}

// AddStringColumn adds a column of string data. The length of the column
// must equal the length of any other columns already present in
// the table.
func (dt *DataTable) AddStringColumn(name string, values []string) error {
	if len(dt.cols) != 0 && len(values) != dt.Len() {
		return ErrInvalidColumnLength
	}
	dt.addColumn(name, colvals{s: values})
	return nil
}

func (dt *DataTable) addColumn(name string, cv colvals) {
	if len(dt.cols) == 0 {
		dt.cols = []colvals{cv}
		dt.colorder = map[string]int{name: 0}
		dt.colnames = []string{name}
		return
	}

	if c, exists := dt.colorder[name]; exists {
		dt.cols[c] = cv
		return
	}

	dt.cols = append(dt.cols, cv)
	dt.colorder[name] = len(dt.cols) - 1
	dt.colnames = append(dt.colnames, name)
}

// RemoveColumn removes a column of any type from the data table.
func (dt *DataTable) RemoveColumn(name string) error {
	if len(dt.cols) == 0 {
		return nil
	}

	c, exists := dt.colorder[name]
	if !exists {
		return fmt.Errorf("unknown column: %s", name)
	}

	// Shift all column positions
	for i := c + 1; i < len(dt.cols); i++ {
		dt.cols[i-1] = dt.cols[i]
		dt.colnames[i-1] = dt.colnames[i]
		dt.colorder[dt.colnames[i]] = i - 1
	}
	dt.cols = dt.cols[:len(dt.cols)-1]
	dt.colnames = dt.colnames[:len(dt.colnames)-1]

	delete(dt.colorder, name)

	// Fix up the keys
	w := 0 // index to copy value into
	for i := range dt.keys {
		v := dt.keys[i]
		switch {
		case v < c:
			dt.keys[w] = dt.keys[i]
			w++
		case v == c:
			// skip this, don't increment w
		case v > c:
			dt.keys[w] = dt.keys[i] - 1
			w++
		}
	}
	dt.keys = dt.keys[:w]
	return nil
}

// Len returns the number of rows in the data table
func (dt *DataTable) Len() int {
	if dt.N() == 0 {
		return 0
	}
	return dt.cols[0].Len()
}

// N returns the number of columns in the data table
func (dt *DataTable) N() int {
	return len(dt.cols)
}

// Row returns a single row of data as a slice or an empty slice and false if the
// row number exceed the bounds of the table. The returned slice contains
// one value per column in the order the columns were added to
// the table.
func (dt *DataTable) Row(n int) ([]interface{}, bool) {
	if n < 0 || n > dt.Len()-1 {
		return []interface{}{}, false
	}
	return dt.row(n), true
}

func (dt *DataTable) RowRef(n int) (RowRef, bool) {
	if n < 0 || n > dt.Len()-1 {
		return RowRef{-1, dt}, false
	}
	return RowRef{n, dt}, true
}

// RowMap returns a single row of data as a map or an empty map and false if the
// row number exceed the bounds of the table. The keys in the returned map
// correspond to the names of the columns.
func (dt *DataTable) RowMap(n int) (RowMap, bool) {
	if n < 0 || n > dt.Len()-1 {
		return RowMap{}, false
	}
	data := make(RowMap, dt.N())
	for name, c := range dt.colorder {
		if dt.cols[c].f != nil {
			data[name] = dt.cols[c].f[n]
		} else {
			data[name] = dt.cols[c].s[n]
		}
	}

	return data, true
}

func (dt *DataTable) row(n int) []interface{} {
	row := make([]interface{}, 0, len(dt.cols))
	for i := 0; i < len(dt.cols); i++ {
		if dt.cols[i].f != nil {
			row = append(row, dt.cols[i].f[n])
		} else {
			row = append(row, dt.cols[i].s[n])
		}
	}
	return row
}

// Names returns a slice of the column names in the data table
// in the order the columns were added to the table.
func (dt *DataTable) Names() []string {
	return dt.colnames
}

// RawRows returns all the rows in the datatable. If headers is true
// then the first row returned will contain the column names. Values
// in each row are in the order the column was added to the table.
func (dt *DataTable) RawRows(headers bool) [][]interface{} {
	if dt.N() == 0 {
		return [][]interface{}{}
	}

	hr := 0
	if headers {
		hr = 1
	}

	ret := make([][]interface{}, dt.Len()+hr)
	if headers {
		for _, name := range dt.colnames {
			ret[0] = append(ret[0], name)
		}
	}

	if dt.Len() == 0 {
		return ret
	}
	for i := 0; i < dt.Len(); i++ {
		ret[i+hr] = dt.row(i)
	}
	return ret
}

// Swap exchanges the data in one row of the table for the data in
// another row.
func (dt *DataTable) Swap(i, j int) {
	for c := range dt.cols {
		if dt.cols[c].f != nil {
			dt.cols[c].f[i], dt.cols[c].f[j] = dt.cols[c].f[j], dt.cols[c].f[i]
		} else {
			dt.cols[c].s[i], dt.cols[c].s[j] = dt.cols[c].s[j], dt.cols[c].s[i]
		}
	}
}

// Less compares two rows and returns whether the row with
// index i should sort before the row at index j.
// If the table has keys specified then only those columns will be used in the
// comparison, in the order specified by the keys. Otherwise all columns are
// compared in the order they were added to the table.
func (dt *DataTable) Less(i, j int) bool {
	if len(dt.keys) == 0 {
		for c := range dt.cols {
			if dt.cols[c].f != nil {
				if dt.cols[c].f[i] == dt.cols[c].f[j] {
					continue
				}
				return dt.cols[c].f[i] < dt.cols[c].f[j]
			}

			if dt.cols[c].s[i] == dt.cols[c].s[j] {
				continue
			}
			return dt.cols[c].s[i] < dt.cols[c].s[j]

		}
		return false
	}
	for _, c := range dt.keys {
		if dt.cols[c].f != nil {
			if dt.cols[c].f[i] == dt.cols[c].f[j] {
				continue
			}
			return dt.cols[c].f[i] < dt.cols[c].f[j]
		}

		if dt.cols[c].s[i] == dt.cols[c].s[j] {
			continue
		}
		return dt.cols[c].s[i] < dt.cols[c].s[j]

	}
	return false
}

// Equal compares two rows and returns whether they contain the same values.
// If the table has keys specified then only those columns will be used in the
// comparison, in the order specified by the keys. Otherwise all columns are
// compared in the order they were added to the table.
func (dt *DataTable) Equal(i, j int) bool {
	if len(dt.keys) == 0 {
		for c := range dt.cols {
			if dt.cols[c].f != nil {
				if dt.cols[c].f[i] != dt.cols[c].f[j] {
					return false
				}
			} else {
				if dt.cols[c].s[i] != dt.cols[c].s[j] {
					return false
				}
			}
		}
		return true
	}
	for _, c := range dt.keys {
		if dt.cols[c].f != nil {
			if dt.cols[c].f[i] != dt.cols[c].f[j] {
				return false
			}
		} else {
			if dt.cols[c].s[i] != dt.cols[c].s[j] {
				return false
			}
		}
	}
	return true
}

// SetKeys assigns a set of column names to be used as keys
// when sorting or aggregating. Setting keys sorts the table
// immediately by the specified keys.
func (dt *DataTable) SetKeys(keys ...string) error {
	keycols := make([]int, len(keys))
keyloop:
	for i, k := range keys {
		for name, col := range dt.colorder {
			if name == k {
				keycols[i] = col
				continue keyloop
			}
		}
		return fmt.Errorf("unknown column: %s", k)
	}

	dt.keys = keycols
	sort.Stable(dt)
	return nil
}

func (dt *DataTable) KeyNames() []string {
	names := make([]string, len(dt.keys))
	for i := range dt.keys {
		for name, col := range dt.colorder {
			if col == dt.keys[i] {
				names[i] = name
				continue
			}
		}
	}
	return names
}

func (dt *DataTable) SetFloatValue(name string, row int, v float64) error {
	if row > dt.Len() {
		return fmt.Errorf("row index out of bounds")
	}
	c, exists := dt.colorder[name]
	if !exists {
		return fmt.Errorf("unknown column: %s", name)
	}

	if !dt.isFloatCol(c) {
		return ErrMismatchedColumnTypes
	}
	dt.cols[c].f[row] = v
	return nil
}

// Calc appends a new numeric column to the table whose values will be
// populated by executing the calculator c against each row of data.
// Rows are evaluated in the table's current sort order as
// specified by its keys.
func (dt *DataTable) Calc(colName string, c Calculator) {
	dt.CalcIndex(colName, c, fillSeq(dt.Len()))
}

// CalcWhere appends a new numeric column to the table whose values will be
// populated by execting the calculator c against each row of data
// that matches m.
// Rows are evaluated in the table's current sort order as
// specified by its keys. Rows not matched by m will be assigned
// a NaN value in the new column.
func (dt *DataTable) CalcWhere(colName string, c Calculator, m Matcher) {
	dt.CalcIndex(colName, c, dt.Matches(m))
}

// CalcIndex appends a new numeric column to the table whose values will be
// populated by execting the calculator c against each row of data
// whose index is contained in indices. Rows are evaluated in the order
// they appear in indices. Rows not present in indices will be assigned
// a NaN value in the new column.
func (dt *DataTable) CalcIndex(colName string, c Calculator, indices []int) {
	col := fillNaN(dt.Len())
	dt.CalcIndexFill(col, c, indices)
	dt.AddColumn(colName, col)
}

func (dt *DataTable) CalcIndexFill(col []float64, c Calculator, indices []int) {
	if dt.Len() == 0 || dt.N() == 0 || len(indices) == 0 || len(col) != dt.Len() {
		return
	}
	rr := RowRef{dt: dt}
	for _, rr.index = range indices {
		col[rr.index] = c.Calculate(rr)
	}

}

// Aggregate appends a new numeric column to the table whose values will be
// populated by executing the aggregator a against each group
// of rows that share the same key column values. Each row in a group
// will be assigned the same value.
// Rows are evaluated in the table's current sort order as
// specified by its keys.
func (dt *DataTable) Aggregate(colName string, a Aggregator) {
	dt.AggregateIndex(colName, a, fillSeq(dt.Len()))
}

// AggregateWhere appends a new numeric column to the table whose values will be
// populated by executing the aggregator a against each group
// of rows that share the same key column values and match m.
// Each row in a group will be assigned the same value.
// Rows are evaluated in the table's current sort order as
// specified by its keys. Rows not matched by m will be assigned
// a NaN value in the new column.
func (dt *DataTable) AggregateWhere(colName string, a Aggregator, m Matcher) {
	dt.AggregateIndex(colName, a, dt.Matches(m))
}

// AggregateIndex appends a new numeric column to the table whose values will be
// populated by executing the aggregator a against each group
// of rows that share the same key column values and are present in indices.
// Each row in a group will be assigned the same value.
// Rows are evaluated in the order they appear in indices. Rows not present
// in indices will be assigned a NaN value in the new column.
func (dt *DataTable) AggregateIndex(colName string, a Aggregator, indices []int) {
	col := fillNaN(dt.Len())
	dt.AggregateIndexFill(col, a, indices)
	dt.AddColumn(colName, col)
}

// AggregateIndexFill populates col with values found by executing the
// aggregator a against each group of rows that share the same key column
// values and are present in indices.
// col must be of the same length as the datatable
func (dt *DataTable) AggregateIndexFill(col []float64, a Aggregator, indices []int) {
	if dt.Len() == 0 || dt.N() == 0 || len(indices) == 0 || len(col) != dt.Len() {
		return
	}

	// This row group will be used to iterate over each identified group. It is
	// reset for each group.
	rg := &StaticRowGroup{dt: dt}

	// Loop through indices identifying groups of rows that share the same key
	// then apply the aggregate function to those rows and use the result as
	// the new column value for each row in the group.
	groupRow := -1
	groupIndex := -1
	for i, row := range indices {
		if groupIndex == -1 {
			groupIndex = i
			groupRow = row
			continue
		}

		if dt.Equal(groupRow, row) {
			continue
		}

		rg.Reset()
		rg.indices = indices[groupIndex:i]
		val := a.Aggregate(rg)
		for j := groupIndex; j < i; j++ {
			col[indices[j]] = val
		}
		groupIndex = i
		groupRow = row
	}

	rg.Reset()
	rg.indices = indices[groupIndex:]
	val := a.Aggregate(rg)
	for j := groupIndex; j < len(indices); j++ {
		col[indices[j]] = val
	}
}

// Apply executes the grouper function g against each group
// of rows that share the same key column values.
// Rows are evaluated in the table's current sort order as
// specified by its keys.
func (dt *DataTable) Apply(g Grouper) {
	dt.ApplyIndex(g, fillSeq(dt.Len()))
}

// ApplyWhere executes the grouper function g against each group
// of rows that share the same key column values and match m.
// Rows are evaluated in the table's current sort order as
// specified by its keys.
func (dt *DataTable) ApplyWhere(g Grouper, m Matcher) {
	if dt.Len() == 0 || dt.N() == 0 || m == nil || g == nil {
		return
	}

	// This row group will be used to iterate over each identified group. It is
	// reset for each group.
	rg := &MatchingRowGroup{dt: dt, matcher: m, start: -1}

	// Loop through indices identifying groups of rows that share the same key
	// then apply the aggregate function to those rows and use the result as
	// the new column value for each row in the group.
	for row := 0; row < dt.Len(); row++ {
		if rg.start == -1 {
			rg.start = row
			rg.Reset()
			continue
		}

		if dt.Equal(rg.start, row) {
			continue
		}
		rg.length = row - rg.start
		g.Group(rg)

		rg.start = row
		rg.Reset()
		rg.length = 0
	}

	rg.length = dt.Len() - rg.start
	g.Group(rg)
}

// ApplyIndex executes the grouper function g against each group
// of rows that share the same key column values and are present in indices.
// Rows are evaluated in the order they appear in indices.
func (dt *DataTable) ApplyIndex(g Grouper, indices []int) {
	if dt.Len() == 0 || dt.N() == 0 || len(indices) == 0 || g == nil {
		return
	}

	// This row group will be used to iterate over each identified group. It is
	// reset for each group.
	rg := &StaticRowGroup{dt: dt}

	// Loop through indices identifying groups of rows that share the same key
	// then apply the aggregate function to those rows and use the result as
	// the new column value for each row in the group.
	groupRow := -1
	groupIndex := -1
	for i, row := range indices {
		if groupIndex == -1 {
			groupIndex = i
			groupRow = row
			continue
		}

		if dt.Equal(groupRow, row) {
			continue
		}

		rg.Reset()
		rg.indices = indices[groupIndex:i]
		g.Group(rg)
		groupIndex = i
		groupRow = row
	}

	rg.Reset()
	rg.indices = indices[groupIndex:]
	g.Group(rg)
}

// Reduce returns the value obtained by executing the
// aggregator a against each row in the datatable.
func (dt *DataTable) Reduce(a Aggregator) float64 {
	return a.Aggregate(dt.Rows())
}

func (dt *DataTable) Rows() RowGroup {
	return &StaticRowGroup{
		dt:      dt,
		indices: fillSeq(dt.Len()),
	}
}

func (dt *DataTable) RowsWhere(m Matcher) RowGroup {
	return &MatchingRowGroup{
		dt:      dt,
		matcher: m,
		length:  dt.Len(),
	}
}

func (dt *DataTable) Matches(m Matcher) []int {
	if dt.Len() == 0 || dt.N() == 0 {
		return []int{}
	}

	rows := make([]int, 0, dt.Len())

	rr := RowRef{dt: dt}
	for rr.index = 0; rr.index < dt.Len(); rr.index++ {
		if m.Match(rr) {
			rows = append(rows, rr.index)
		}
	}
	return rows
}

// CountWhere counts the number of rows that match m.
// Rows are evaluated in the table's current sort order as
// specified by its keys.
func (dt *DataTable) CountWhere(m Matcher) int {
	if dt.Len() == 0 || dt.N() == 0 {
		return 0
	}

	count := 0
	rr := RowRef{dt: dt}
	for rr.index = 0; rr.index < dt.Len(); rr.index++ {
		if m.Match(rr) {
			count++
		}

	}
	return count
}

// RemoveRows removes any rows that match m without altering their order.
func (dt *DataTable) RemoveRows(m Matcher) {
	if dt.Len() == 0 || dt.N() == 0 {
		return
	}

	matches := dt.Matches(m)
	if len(matches) == 0 {
		// Nothing to do
		return
	}

	for i := len(matches) - 1; i >= 0; i-- {

		p := matches[i]
		for c := range dt.cols {
			if dt.cols[c].f != nil {
				dt.cols[c].f = append(dt.cols[c].f[0:p], dt.cols[c].f[p+1:]...)
			} else {
				dt.cols[c].s = append(dt.cols[c].s[0:p], dt.cols[c].s[p+1:]...)
			}
		}
	}
}

// ParseRow attempts to append a row of data by parsing values
// as either float64 or string depending on the existing type
// of the relevant column. Values are processed in the order
// that columns were added to the table.
func (dt *DataTable) ParseRow(values ...string) error {
	if len(values) != dt.N() {
		return ErrWrongNumberOfColumns
	}

	for i := 0; i < len(values); i++ {
		if dt.isFloatCol(i) {
			v, err := strconv.ParseFloat(values[i], 64)
			if err != nil {
				return fmt.Errorf("%v (column %d)", err, i)
			}
			dt.cols[i].f = append(dt.cols[i].f, v) // TODO: don't add until all values have been parsed
		} else {
			dt.cols[i].s = append(dt.cols[i].s, values[i])
		}
	}

	return nil
}

// Append appends the rows of dt2 to the data table. An error
// is returned if the tables share a column name with differing
// types (numeric vs text). Columns present in dt but not in
// dt2 will be expanded to the correct length with either NaN or
// the empty string. Columns present in dt2 but not dt will be
// pre-filled with NaN or empty strings before the dt2's data is
// appened.
// The data table remains sorted according to its keys after the
// append.
func (dt *DataTable) Append(dt2 *DataTable) error {
	currentLen := dt.Len()
	for name, c2 := range dt2.colorder {
		c, exists := dt.colorder[name]

		// Column in dt2 but not in dt
		if !exists {
			// New column so fill with NaN or empty string first
			// then append new values
			if dt2.cols[c2].f != nil {
				values := fillNaN(currentLen)
				values = append(values, dt2.cols[c2].f...)
				dt.addColumn(name, colvals{f: values})
				continue
			} else {
				values := make([]string, currentLen)
				values = append(values, dt2.cols[c2].s...)
				dt.addColumn(name, colvals{s: values})
				continue
			}
		}

		// Column in both dt and dt2
		if dt.cols[c].f != nil && dt2.cols[c2].f != nil {
			dt.cols[c].f = append(dt.cols[c].f, dt2.cols[c2].f...)
			continue
		}

		if dt.cols[c].s != nil && dt2.cols[c2].s != nil {
			dt.cols[c].s = append(dt.cols[c].s, dt2.cols[c2].s...)
			continue
		}

		return ErrMismatchedColumnTypes

	}

	// Now pad out any columns that are in dt but not dt2
	for name, c := range dt.colorder {
		if _, exists := dt2.colorder[name]; !exists {
			if dt.cols[c].f != nil {
				dt.cols[c].f = append(dt.cols[c].f, fillNaN(dt2.Len())...)
			} else {
				dt.cols[c].s = append(dt.cols[c].s, make([]string, dt2.Len())...)
			}
		}
	}

	// Keep dt sorted
	if len(dt.keys) > 0 {
		sort.Stable(dt)
	}

	return nil
}

// Select returns a new data table containing copies of the columns
// specified in names. The returned data table will have no keys
// set.
func (dt *DataTable) Select(names []string) (*DataTable, error) {
	dt2 := &DataTable{}
	for _, name := range names {
		c, exists := dt.colorder[name]
		if !exists {
			return nil, fmt.Errorf("unknown column: %s", name)
		}

		if dt.cols[c].f != nil {
			values := make([]float64, len(dt.cols[c].f))
			copy(values, dt.cols[c].f)
			dt2.addColumn(name, colvals{f: values})
		} else {
			values := make([]string, len(dt.cols[c].s))
			copy(values, dt.cols[c].s)
			dt2.addColumn(name, colvals{s: values})
		}
	}

	return dt2, nil
}

// SelectWhere returns a new data table containing copies of the columns
// specified in names where the rows match m. The returned data table
// will have no keys set.
func (dt *DataTable) SelectWhere(names []string, m Matcher) (*DataTable, error) {
	return dt.SelectIndex(names, dt.Matches(m))
}

// SelectIndex returns a new data table containing copies of the columns
// specified in names where the rows are in indices. The returned data table
// will have no keys set.
func (dt *DataTable) SelectIndex(names []string, indices []int) (*DataTable, error) {
	dt2 := &DataTable{}

	for _, name := range names {
		c, exists := dt.colorder[name]
		if !exists {
			return nil, fmt.Errorf("unknown column: %s", name)
		}

		if dt.cols[c].f != nil {
			dt2.addColumn(name, colvals{f: make([]float64, len(indices))})
		} else {
			dt2.addColumn(name, colvals{s: make([]string, len(indices))})
		}
	}

	for i, idx := range indices {
		for _, name := range names {
			c, _ := dt.colorder[name]
			c2, _ := dt2.colorder[name]
			if dt.cols[c].f != nil {
				dt2.cols[c2].f[i] = dt.cols[c].f[idx]
			} else {
				dt2.cols[c2].s[i] = dt.cols[c].s[idx]
			}
		}
	}

	return dt2, nil
}

// Unique returns a new data table containing only the
// unique rows from dt. The returned data table will
// contain the same number of columns in the same order
// as dt and will have no keys set.
func (dt *DataTable) Unique() *DataTable {
	dt2 := &DataTable{
		colorder: map[string]int{},
	}
	if dt.Len() == 0 {
		return dt2
	}

	prevKeys := dt.keys
	// remove any sort keys and sort in natural order
	dt.keys = []int{}
	sort.Stable(dt)

	for c := range dt.cols {
		dt2.colnames = append(dt2.colnames, dt.colnames[c])
		dt2.colorder[dt.colnames[c]] = c
		if dt.cols[c].f != nil {
			dt2.cols = append(dt2.cols, colvals{f: []float64{dt.cols[c].f[0]}})
		} else {
			dt2.cols = append(dt2.cols, colvals{s: []string{dt.cols[c].s[0]}})
		}
	}

rowloop:
	for i := 1; i < dt.Len(); i++ {
		for c := 0; c < len(dt.cols); c++ {
			if dt.cols[c].f != nil {
				if dt.cols[c].f[i] != dt.cols[c].f[i-1] {
					copyRow(dt, dt2, i)
					continue rowloop
				}
			} else {
				if dt.cols[c].s[i] != dt.cols[c].s[i-1] {
					copyRow(dt, dt2, i)
					continue rowloop
				}
			}
		}
	}

	// Restore previous sort order, if any
	if len(prevKeys) > 0 {
		dt.keys = prevKeys
		sort.Stable(dt)
	}

	return dt2
}

// CloneEmpty creates an identical but empty data table with no keys set.
func (dt *DataTable) CloneEmpty() *DataTable {
	dt2 := &DataTable{
		colorder: map[string]int{},
		keys:     []int{},
	}
	if dt.Len() == 0 {
		return dt2
	}

	for c := range dt.cols {
		if dt.cols[c].f != nil {
			dt2.AddColumn(dt.colnames[c], []float64{})
		} else {
			dt2.AddStringColumn(dt.colnames[c], []string{})
		}
	}

	return dt2
}

// Clone returns a new data table containing copies of the columns
// contained in dt. The returned data table will have no keys
// set.
func (dt *DataTable) Clone() *DataTable {
	dtClone, _ := dt.Select(dt.Names())
	return dtClone
}

// copyRow copies a row from one data table to another. It does not check whether the
// row number is in bounds and assumes that both tables have the same columns in the
// same order
func copyRow(dt, dt2 *DataTable, n int) {
	for c := range dt.cols {
		if dt.cols[c].f != nil {
			dt2.cols[c].f = append(dt2.cols[c].f, dt.cols[c].f[n])
		} else {
			dt2.cols[c].s = append(dt2.cols[c].s, dt.cols[c].s[n])
		}
	}
}

// AppendRow appends the data in row to the data table.
func (dt *DataTable) AppendRow(row []interface{}) error {
	if len(row) != dt.N() {
		return ErrWrongNumberOfColumns
	}
	for c := range dt.cols {
		if dt.isFloatCol(c) {
			v, ok := row[c].(float64)
			if !ok {
				return ErrMismatchedColumnTypes
			}
			dt.cols[c].f = append(dt.cols[c].f, v)
		} else {
			v, ok := row[c].(string)
			if !ok {
				return ErrMismatchedColumnTypes
			}
			dt.cols[c].s = append(dt.cols[c].s, v)
		}
	}
	return nil
}

func (dt *DataTable) isFloatCol(c int) bool {
	return dt.cols[c].f != nil
}

// CSV writes the datatable as CSV
func (dt *DataTable) CSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	for _, row := range dt.RawRows(true) {
		sw := make([]string, len(row))
		for i := range row {
			sw[i] = fmt.Sprintf("%v", row[i])
		}
		err := cw.Write(sw)
		if err != nil {
			return fmt.Errorf("writing csv row: %v", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("writing csv row: %v", err)
	}
	return nil
}

type Aggregator interface {
	Aggregate(rg RowGroup) float64
}

// AggregatorFunc adapts a function to an Aggregator interface
type AggregatorFunc func(rg RowGroup) float64

func (fn AggregatorFunc) Aggregate(rg RowGroup) float64 {
	return fn(rg)
}

// Sum returns an Aggregator that sums a numeric column in a group of rows.
func Sum(name string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		r := 0.0
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			r += v
		}
		return r
	})
}

// Max returns an Aggregator that finds the maximum value of a numeric column in a group of rows.
func Max(name string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		max := 0.0
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			if v > max {
				max = v
			}
		}
		return max
	})
}

// Min returns an Aggregator that finds the minimum value of a numeric column in a group of rows.
func Min(name string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		min := 0.0
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			if v < min {
				min = v
			}
		}
		return min
	})
}

// Count returns an Aggregator that finds the count of numeric values in a group of rows.
func Count() Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		count := 0
		for rg.Next() {
			count++
		}
		return float64(count)
	})
}

// Mean returns an Aggregator that finds the mean value of a numeric column in a group of rows.
func Mean(name string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		sum := 0.0
		count := 0
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			sum += v
			count++
		}
		return sum / float64(count)
	})
}

// Variance returns an Aggregator that finds the variance of a numeric column in a group of rows.
func Variance(name string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {

		// Based on MeanVariance from github.com/gonum/stat
		// This uses the corrected two-pass algorithm (1.7), from "Algorithms for computing
		// the sample variance: Analysis and recommendations" by Chan, Tony F., Gene H. Golub,
		// and Randall J. LeVeque.
		sum := 0.0
		count := 0
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			sum += v
			count++
		}
		mean := sum / float64(count)

		var (
			ss           float64
			compensation float64
		)
		rg.Reset()
		for rg.Next() {
			v, _ := rg.FloatValue(name)
			d := v - mean
			ss += d * d
			compensation += d
		}
		return (ss - compensation*compensation/float64(count)) / float64(count-1)

	})
}

func RatioOfSums(a, b string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		suma, sumb := 0.0, 0.0
		for rg.Next() {
			va, _ := rg.FloatValue(a)
			suma += va

			vb, _ := rg.FloatValue(b)
			sumb += vb
		}
		return suma / sumb
	})
}

func DifferenceOfSums(a, b string) Aggregator {
	return AggregatorFunc(func(rg RowGroup) float64 {
		suma, sumb := 0.0, 0.0
		for rg.Next() {
			va, _ := rg.FloatValue(a)
			suma += va

			vb, _ := rg.FloatValue(b)
			sumb += vb
		}
		return suma - sumb
	})
}

// A Matcher tests a single row of data to determine
// whether it matches a particular set of criteria.
type Matcher interface {
	Match(row RowRef) bool
}

// MatcherFunc adapts a function to a Matcher interface
type MatcherFunc func(row RowRef) bool

func (fn MatcherFunc) Match(row RowRef) bool {
	return fn(row)
}

// NumericColumnMatcher returns a Matcher that tests the value of
// a single column in a row of data against the numeric function fn.
func NumericColumnMatcher(name string, fn func(float64) bool) Matcher {
	return MatcherFunc(func(row RowRef) bool {
		if v, exists := row.FloatValue(name); exists {
			return fn(v)
		}
		return false
	})
}

// IsZero returns a Matcher that tests whether the named column is zero or not
func IsZero(name string) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return f == 0.0 })
}

// IsNan returns a Matcher that tests whether the named column is NaN or not
func IsNan(name string) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return math.IsNaN(f) })
}

// IsInf returns a Matcher that tests whether the named column is infinite (either positive
// or negative infinity will return true).
func IsInf(name string) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return math.IsInf(f, 0) })
}

// GreaterThan returns a Matcher that tests whether the named column is greater than v or not
func GreaterThan(name string, v float64) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return f > v })
}

// LessThan returns a Matcher that tests whether the named column is less than v or not
func LessThan(name string, v float64) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return f < v })
}

// CloselyEqual returns a Matcher that tests whether the named column is equal to v within the range +/- e
func CloselyEqual(name string, v float64, e float64) Matcher {
	return NumericColumnMatcher(name, func(f float64) bool { return f == v || math.Abs(f-v) <= e })
}

// StringColumnMatcher returns a Matcher that tests the value of
// a single column in a row of data against the string function fn.
func StringColumnMatcher(name string, fn func(string) bool) Matcher {
	return MatcherFunc(func(row RowRef) bool {
		if v, exists := row.StringValue(name); exists {
			return fn(v)
		}
		return false
	})
}

// IsEqualString returns a Matcher that tests whether the named column is equal to the
// given string
func IsEqualString(col string, val string) Matcher {
	return StringColumnMatcher(col, func(s string) bool { return s == val })
}

// Not returns a Matcher that inverts the value of the supplied matcher
func Not(m Matcher) Matcher {
	return MatcherFunc(func(row RowRef) bool {
		return !m.Match(row)
	})
}

// MultiColumnMatcher returns a Matcher that tests whether the a rown matches
// the names and values  in the map m
func MultiColumnMatcher(m map[string]string) Matcher {
	return MatcherFunc(func(row RowRef) bool {
		for name, wanted := range m {
			if v, exists := row.StringValue(name); !exists || v != wanted {
				return false
			}
		}
		return true
	})
}

// A Calculator performs a calculation on a single row of numeric data.
type Calculator interface {
	Calculate(row RowRef) float64
}

// CalculatorFunc adapts a function to a Calculator interface
type CalculatorFunc func(row RowRef) float64

func (fn CalculatorFunc) Calculate(row RowRef) float64 {
	return fn(row)
}

// Zero returns a Calculator that always returns zero
func Zero() Calculator {
	return Constant(0)
}

// Constant returns a Calculator that always returns the constant value v
func Constant(v float64) Calculator {
	return CalculatorFunc(func(row RowRef) float64 { return v })
}

// A Grouper performs an action given a group of rows.
type Grouper interface {
	Group(rg RowGroup)
}

// GrouperFunc adapts a function to a Grouper interface
type GrouperFunc func(rg RowGroup)

func (fn GrouperFunc) Group(rg RowGroup) {
	fn(rg)
}

func fillNaN(n int) []float64 {
	ret := make([]float64, n)
	for i := 0; i < n; i++ {
		ret[i] = math.NaN()
	}
	return ret
}

func fillSeq(n int) []int {
	ret := make([]int, n)
	for i := 0; i < n; i++ {
		ret[i] = i
	}
	return ret
}

// A Valuer can get the value of a column in
// a particular context
type Valuer interface {
	Value(name string) (interface{}, bool)
	FloatValue(name string) (float64, bool)
	StringValue(name string) (string, bool)
}

type RowGroup interface {
	Valuer
	Reset()
	RowIndex() int
	Next() bool
}

type StaticRowGroup struct {
	indices []int
	offset  int // one greater than the current index into indices
	dt      *DataTable
}

func (r *StaticRowGroup) Reset() {
	r.offset = 0
}

// RowIndex returns the datatable index of the current row in the
// row group. It is an error if this is called before calling Next
// and the function will panic.
func (r *StaticRowGroup) RowIndex() int {
	return r.indices[r.offset-1]
}

func (r *StaticRowGroup) Next() bool {
	r.offset++
	return r.offset <= len(r.indices)
}

func (r *StaticRowGroup) Value(name string) (interface{}, bool) {
	if c, exists := r.dt.colorder[name]; exists {
		n := r.indices[r.offset-1]
		if r.dt.cols[c].f != nil {
			return r.dt.cols[c].f[n], true
		}
		return r.dt.cols[c].s[n], true
	}
	return nil, false
}

func (r *StaticRowGroup) FloatValue(name string) (float64, bool) {
	if c, exists := r.dt.colorder[name]; exists && r.dt.cols[c].f != nil {
		n := r.indices[r.offset-1]
		return r.dt.cols[c].f[n], true
	}
	return 0, false
}

func (r *StaticRowGroup) StringValue(name string) (string, bool) {
	if c, exists := r.dt.colorder[name]; exists && r.dt.cols[c].s != nil {
		n := r.indices[r.offset-1]
		return r.dt.cols[c].s[n], true
	}
	return "", false
}

// Where applies a matcher to the rows in this row group, returning a new
// row group that contains only the rows that matched. It does not affect
// the current position of r's iteration.
func (r *StaticRowGroup) Where(m Matcher) *StaticRowGroup {
	matches := make([]int, 0, len(r.indices))

	rr := RowRef{dt: r.dt}
	for _, rr.index = range r.indices {
		if m.Match(rr) {
			matches = append(matches, rr.index)
		}
	}

	return &StaticRowGroup{
		dt:      r.dt,
		indices: matches,
	}
}

type MatchingRowGroup struct {
	start   int
	next    int // the next row to check, one greater than the current row
	length  int // the maximum number number of rows to check
	dt      *DataTable
	matcher Matcher
}

func (m *MatchingRowGroup) Reset() {
	m.next = m.start
}

func (m *MatchingRowGroup) RowIndex() int {
	return m.next - 1
}

func (m *MatchingRowGroup) Next() bool {
	rr := RowRef{dt: m.dt}
	for rr.index = m.next; rr.index < m.dt.Len() && rr.index < m.start+m.length; rr.index++ {
		if m.matcher.Match(rr) {
			m.next = rr.index + 1
			return true
		}
	}
	return false
}

func (m *MatchingRowGroup) Value(name string) (interface{}, bool) {
	if c, exists := m.dt.colorder[name]; exists {
		if m.dt.cols[c].f != nil {
			return m.dt.cols[c].f[m.next-1], true
		}
		return m.dt.cols[c].s[m.next-1], true
	}
	return nil, false
}

func (m *MatchingRowGroup) FloatValue(name string) (float64, bool) {
	if c, exists := m.dt.colorder[name]; exists && m.dt.cols[c].f != nil {
		return m.dt.cols[c].f[m.next-1], true
	}
	return 0, false
}
func (m *MatchingRowGroup) StringValue(name string) (string, bool) {
	if c, exists := m.dt.colorder[name]; exists && m.dt.cols[c].s != nil {
		return m.dt.cols[c].s[m.next-1], true
	}
	return "", false
}

type RowRef struct {
	index int
	dt    *DataTable
}

func (r *RowRef) Value(name string) (interface{}, bool) {
	if c, exists := r.dt.colorder[name]; exists {
		if r.dt.cols[c].f != nil {
			return r.dt.cols[c].f[r.index], true
		}
		return r.dt.cols[c].s[r.index], true
	}
	return nil, false
}

func (r *RowRef) FloatValue(name string) (float64, bool) {
	if c, exists := r.dt.colorder[name]; exists && r.dt.cols[c].f != nil {
		return r.dt.cols[c].f[r.index], true
	}
	return 0, false
}

func (r *RowRef) StringValue(name string) (string, bool) {
	if c, exists := r.dt.colorder[name]; exists && r.dt.cols[c].s != nil {
		return r.dt.cols[c].s[r.index], true
	}
	return "", false
}

type RowMap map[string]interface{}

func (r RowMap) Value(name string) (interface{}, bool) {
	if r == nil {
		return nil, false
	}
	v, ok := r[name]
	return v, ok
}

func (r RowMap) FloatValue(name string) (float64, bool) {
	if r == nil {
		return 0, false
	}
	if v, ok := r[name]; ok {
		if vf, ok := v.(float64); ok {
			return vf, true
		}
	}
	return 0, false
}

func (r RowMap) StringValue(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	if v, ok := r[name]; ok {
		if vs, ok := v.(string); ok {
			return vs, true
		}
	}
	return "", false
}
