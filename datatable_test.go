package datatable

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestAddColumn(t *testing.T) {
	dt := &DataTable{}

	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	t.Logf("%+v", dt.cols[0])
	if dt.Len() != 5 {
		t.Errorf("got %d, wanted %d", dt.Len(), 5)
	}
}

func TestRow(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	expectedRows := [][]float64{
		{5, 8},
		{4, 9},
		{3, 10},
		{2, 9},
		{1, 8},
	}

	for i, expectedRow := range expectedRows {
		row, _ := dt.Row(i)
		if len(row) != len(expectedRow) {
			t.Fatalf("got %d, wanted %d", len(row), len(expectedRow))
		}
		for j := range expectedRow {
			if row[j] != expectedRow[j] {
				t.Errorf("got %f, wanted %f", row[j], expectedRow[j])
			}
		}
	}
}

func TestSwap(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	dt.Swap(0, 2)
	row2, _ := dt.Row(2)
	if !reflect.DeepEqual(row2, []interface{}{5.0, 8.0}) {
		t.Errorf("got %+v, wanted %+v", row2, []interface{}{5.0, 8.0})
	}

	row0, _ := dt.Row(0)
	if !reflect.DeepEqual(row0, []interface{}{3.0, 10.0}) {
		t.Errorf("got %+v, wanted %+v", row0, []interface{}{3.0, 10.0})
	}
}

func TestLessNoKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	testCases := []struct {
		row1 int
		row2 int
		less bool
	}{
		{0, 1, false},
		{1, 2, false},
		{2, 3, false},
		{3, 4, false},
		{4, 3, true},
		{4, 0, true},
	}

	for i, tc := range testCases {
		less := dt.Less(tc.row1, tc.row2)
		if less != tc.less {
			t.Errorf("%d:got %v, wanted %v", i, less, tc.less)
		}
	}
}

func TestLessNoKeysColumnWise(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{1, 2, 3, 4, 5})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	testCases := []struct {
		row1 int
		row2 int
		less bool
	}{
		{0, 1, true},
		{1, 2, true},
		{2, 3, true},
		{3, 4, true},
		{4, 3, false},
		{4, 0, false},
	}

	for i, tc := range testCases {
		less := dt.Less(tc.row1, tc.row2)
		if less != tc.less {
			t.Errorf("%d: got %v, wanted %v", i, less, tc.less)
		}
	}
}

func TestLessWithKey(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})
	dt.SetKeys("test2") // automatically sorts by the key

	testCases := []struct {
		row1 int
		row2 int
		less bool
	}{
		{0, 1, false}, // 8 vs 8
		{1, 2, true},  // 8 vs 9
		{2, 3, false}, // 9 vs 9
		{3, 4, true},  // 9 vs 10
		{4, 3, false}, // 10 vs 9
		{4, 0, false}, // 10 vs 8
	}

	for _, r := range dt.RawRows(true) {
		t.Logf("%+v", r)
	}

	for i, tc := range testCases {
		less := dt.Less(tc.row1, tc.row2)
		if less != tc.less {
			t.Errorf("%d: got %v, wanted %v", i, less, tc.less)
		}
	}
}

func TestSortNoKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	expectedRows := [][]float64{
		{1, 8},
		{2, 9},
		{3, 10},
		{4, 9},
		{5, 8},
	}

	sort.Sort(dt)

	for i, expectedRow := range expectedRows {
		row, _ := dt.Row(i)
		t.Logf("row %d: %+v", i, row)
		if len(row) != len(expectedRow) {
			t.Fatalf("got %d, wanted %d", len(row), len(expectedRow))
		}
		for j := range expectedRow {
			if row[j] != expectedRow[j] {
				t.Errorf("got %f, wanted %f", row[j], expectedRow[j])
			}
		}
	}
}

func TestAggregateNoKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	dt.Aggregate("sum", Sum("test"))

	expectedRows := [][]float64{
		{5, 8, 5},
		{4, 9, 4},
		{3, 10, 3},
		{2, 9, 2},
		{1, 8, 1},
	}

	for i, expectedRow := range expectedRows {
		row, _ := dt.Row(i)
		t.Logf("row %d: %+v", i, row)
		if len(row) != len(expectedRow) {
			t.Fatalf("got %d, wanted %d", len(row), len(expectedRow))
		}
		for j := range expectedRow {
			if row[j] != expectedRow[j] {
				t.Errorf("got %f, wanted %f", row[j], expectedRow[j])
			}
		}
	}
}

func TestAggregateWithKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})
	dt.SetKeys("test2") // automatically sorts by the key

	dt.Aggregate("sum", Sum("test"))

	expectedRows := [][]float64{
		{5, 8, 6},
		{1, 8, 6},
		{4, 9, 6},
		{2, 9, 6},
		{3, 10, 3},
	}

	for i, expectedRow := range expectedRows {
		row, _ := dt.Row(i)
		t.Logf("row %d: %+v", i, row)
		if len(row) != len(expectedRow) {
			t.Fatalf("got %d, wanted %d", len(row), len(expectedRow))
		}
		for j := range expectedRow {
			if row[j] != expectedRow[j] {
				t.Errorf("got %f, wanted %f", row[j], expectedRow[j])
			}
		}
	}
}

func TestRemoveRows(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})

	fn := func(row RowRef) bool { v, exists := row.FloatValue("test2"); return exists && v == 8.0 }
	dt.RemoveRows(MatcherFunc(fn))

	expectedRows := [][]interface{}{
		{4.0, 9.0},
		{3.0, 10.0},
		{2.0, 9.0},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func doBenchmarkRemoveRows(dt *DataTable, m Matcher, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := dt.Clone()
		b.StartTimer()
		c.RemoveRows(m)
	}
}

func BenchmarkRemoveRowsSmallLowNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 100), GreaterThan("c0", 0.95), b)
}

func BenchmarkRemoveRowsSmallMedNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 100), GreaterThan("c0", 0.5), b)
}

func BenchmarkRemoveRowsSmallHighNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 100), GreaterThan("c0", 0.05), b)
}

func BenchmarkRemoveRowsMedLowNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 1000), GreaterThan("c0", 0.95), b)
}

func BenchmarkRemoveRowsMedMedNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 1000), GreaterThan("c0", 0.5), b)
}

func BenchmarkRemoveRowsMedHighNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 1000), GreaterThan("c0", 0.05), b)
}

func BenchmarkRemoveRowsBigLowNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 10000), GreaterThan("c0", 0.95), b)
}

func BenchmarkRemoveRowsBigMedNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 10000), GreaterThan("c0", 0.5), b)
}

func BenchmarkRemoveRowsBigHighNumeric(b *testing.B) {
	doBenchmarkRemoveRows(makeTable(3, 10000), GreaterThan("c0", 0.05), b)
}

func TestAggregateWhere(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})
	dt.SetKeys("test2") // automatically sorts by the key

	fn := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("test2"); return exists && v != 9.0 })
	dt.AggregateWhere("sum", Sum("test"), fn)

	expectedRows := [][]interface{}{
		{5.0, 8.0, 6.0},
		{1.0, 8.0, 6.0},
		{4.0, 9.0, math.NaN()},
		{2.0, 9.0, math.NaN()},
		{3.0, 10.0, 3.0},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func equivalentFloats(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsInf(a, -1) && math.IsInf(b, -1) {
		return true
	}
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	return a == b
}

func equivalentFloatSlices(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equivalentFloats(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equivalentRows(a, b [][]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}

		for j := 0; j < len(a[i]); j++ {
			switch aval := a[i][j].(type) {
			case string:
				bval, ok := b[i][j].(string)
				if !ok {
					return false
				}
				if aval != bval {
					return false
				}
			case float64:
				bval, ok := b[i][j].(float64)
				if !ok {
					return false
				}
				if !equivalentFloats(aval, bval) {
					return false
				}
			default:
				return false
			}
		}

	}

	return true
}

func TestAppendSameColsNoKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3})
	dt.AddColumn("test2", []float64{8, 9, 10})

	dt2 := &DataTable{}
	dt2.AddColumn("test", []float64{2, 1})
	dt2.AddColumn("test2", []float64{9, 8})

	expectedRows := [][]interface{}{
		{5.0, 8.0},
		{4.0, 9.0},
		{3.0, 10.0},
		{2.0, 9.0},
		{1.0, 8.0},
	}

	err := dt.Append(dt2)
	if err != nil {
		t.Fatalf(err.Error())
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestAppendDifferentColsNoKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3})
	dt.AddColumn("test2", []float64{8, 9, 10})

	dt2 := &DataTable{}
	dt2.AddColumn("test", []float64{2, 1})
	dt2.AddColumn("test3", []float64{9, 8})

	expectedRows := [][]interface{}{
		{5.0, 8.0, math.NaN()},
		{4.0, 9.0, math.NaN()},
		{3.0, 10.0, math.NaN()},
		{2.0, math.NaN(), 9.0},
		{1.0, math.NaN(), 8.0},
	}

	err := dt.Append(dt2)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("%+v", dt.cols)

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestAppendSameColsWithKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3})
	dt.AddColumn("test2", []float64{8, 9, 10})
	dt.SetKeys("test2")

	dt2 := &DataTable{}
	dt2.AddColumn("test", []float64{2, 1})
	dt2.AddColumn("test2", []float64{9, 8})

	expectedRows := [][]interface{}{
		{5.0, 8.0},
		{1.0, 8.0},
		{4.0, 9.0},
		{2.0, 9.0},
		{3.0, 10.0},
	}

	err := dt.Append(dt2)
	if err != nil {
		t.Fatalf(err.Error())
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestSelect(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3})
	dt.AddColumn("test2", []float64{8, 9, 10})
	dt.AddStringColumn("label", []string{"a", "b", "c"})

	expectedRows := [][]interface{}{
		{"a", 5.0},
		{"b", 4.0},
		{"c", 3.0},
	}

	dt2, err := dt.Select([]string{"label", "test"})
	if err != nil {
		t.Fatalf(err.Error())
	}

	rows := dt2.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestSelectWhere(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3})
	dt.AddColumn("test2", []float64{8, 9, 10})
	dt.AddStringColumn("label", []string{"a", "b", "c"})

	expectedRows := [][]interface{}{
		{"a", 5.0},
		{"c", 3.0},
	}

	fn := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("test2"); return exists && v != 9.0 })
	dt2, err := dt.SelectWhere([]string{"label", "test"}, fn)
	if err != nil {
		t.Fatalf(err.Error())
	}

	rows := dt2.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestUnique(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 5, 4})
	dt.AddColumn("test2", []float64{8, 9, 8, 9})
	dt.AddStringColumn("label", []string{"a", "b", "a", "b"})

	expectedRows := [][]interface{}{
		{4.0, 9.0, "b"},
		{5.0, 8.0, "a"},
	}

	dt2 := dt.Unique()

	rows := dt2.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestCalcWhere(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})
	dt.SetKeys("test2") // automatically sorts by the key

	matcher := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("test2"); return exists && v != 9.0 })
	calc := CalculatorFunc(func(row RowRef) float64 { v, _ := row.FloatValue("test"); return v * 2 })

	dt.CalcWhere("calc", calc, matcher)

	expectedRows := [][]interface{}{
		{5.0, 8.0, 10.0},
		{1.0, 8.0, 2.0},
		{4.0, 9.0, math.NaN()},
		{2.0, 9.0, math.NaN()},
		{3.0, 10.0, 6.0},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestMatches(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("c0", []float64{
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
	})

	expected := []int{2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57, 62, 67, 72, 77, 82, 87, 92, 97}

	matcher := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("c0"); return exists && v == 3.0 })

	matches := dt.Matches(matcher)
	if !reflect.DeepEqual(matches, expected) {
		t.Errorf("got %+v, wanted %+v", matches, expected)
	}
}

func TestCalcWhereEmptyTable(t *testing.T) {
	dt := &DataTable{}

	matcher := MatcherFunc(func(row RowRef) bool { return true })
	calc := CalculatorFunc(func(row RowRef) float64 { return 1 })

	dt.CalcWhere("calc", calc, matcher)

	if dt.Len() != 0 {
		t.Errorf("got %d rows, wanted 0", dt.Len())
	}

	if dt.N() != 1 {
		t.Errorf("got %d cols, wanted 1", dt.N())
	}
}

func TestCalcWhereZeroRows(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{})

	matcher := MatcherFunc(func(row RowRef) bool { return true })
	calc := CalculatorFunc(func(row RowRef) float64 { return 1 })

	dt.CalcWhere("calc", calc, matcher)

	if dt.Len() != 0 {
		t.Errorf("got %d rows, wanted 0", dt.Len())
	}

	if dt.N() != 2 {
		t.Errorf("got %d cols, wanted 2", dt.N())
	}
}

func TestCalcWhereNoMatches(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{1, 2, 3, 4})

	matcher := MatcherFunc(func(row RowRef) bool { return false })
	calc := CalculatorFunc(func(row RowRef) float64 { return 1 })

	dt.CalcWhere("calc", calc, matcher)

	expectedRows := [][]interface{}{
		{1.0, math.NaN()},
		{2.0, math.NaN()},
		{3.0, math.NaN()},
		{4.0, math.NaN()},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestCalcReplacesColumnWithSameName(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{8, 9, 10, 9, 8})
	dt.SetKeys("test2") // automatically sorts by the key

	matcher := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("test2"); return exists && v != 9.0 })
	calc := CalculatorFunc(func(row RowRef) float64 { v, _ := row.FloatValue("test"); return v * 2 })

	dt.CalcWhere("test2", calc, matcher)

	expectedRows := [][]interface{}{
		{5.0, 10.0},
		{1.0, 2.0},
		{4.0, math.NaN()},
		{2.0, math.NaN()},
		{3.0, 6.0},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestAggregateWhereEmptyTable(t *testing.T) {
	dt := &DataTable{}

	matcher := MatcherFunc(func(row RowRef) bool { return true })
	dt.AggregateWhere("sum", Sum("test"), matcher)

	if dt.Len() != 0 {
		t.Errorf("got %d rows, wanted 0", dt.Len())
	}

	if dt.N() != 1 {
		t.Errorf("got %d cols, wanted 1", dt.N())
	}
}

func TestAggregateWhereZeroRows(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{})

	matcher := MatcherFunc(func(row RowRef) bool { return true })
	dt.AggregateWhere("sum", Sum("test"), matcher)

	if dt.Len() != 0 {
		t.Errorf("got %d rows, wanted 0", dt.Len())
	}

	if dt.N() != 2 {
		t.Errorf("got %d cols, wanted 2", dt.N())
	}
}

func TestAggregateWhereNoMatches(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{1, 2, 3, 4})

	matcher := MatcherFunc(func(row RowRef) bool { return false })
	dt.AggregateWhere("sum", Sum("test"), matcher)

	expectedRows := [][]interface{}{
		{1.0, math.NaN()},
		{2.0, math.NaN()},
		{3.0, math.NaN()},
		{4.0, math.NaN()},
	}

	rows := dt.RawRows(false)
	if !equivalentRows(rows, expectedRows) {
		t.Errorf("got %+v, wanted %+v", rows, expectedRows)
	}
}

func TestRemoveColumn(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{5, 4, 3, 2, 1})

	dt.RemoveColumn("test")

	if dt.N() != 1 {
		t.Errorf("got %d, wanted %d", dt.N(), 1)
	}
}

func TestRemoveColumnEmptyTable(t *testing.T) {
	dt := &DataTable{}
	err := dt.RemoveColumn("test")
	if err != nil {
		t.Errorf("got %v, wanted no error", err)
	}
	if dt.N() != 0 {
		t.Errorf("got %d, wanted %d", dt.N(), 0)
	}
}

func TestRemoveColumnOneCol(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	err := dt.RemoveColumn("test")
	if err != nil {
		t.Errorf("got %v, wanted no error", err)
	}
	if dt.N() != 0 {
		t.Errorf("got %d, wanted %d", dt.N(), 0)
	}
}

func TestRemoveColumnKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test1", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test3", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test4", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test5", []float64{5, 4, 3, 2, 1})

	dt.SetKeys("test2", "test5", "test4", "test1")

	expectedBefore := []int{1, 4, 3, 0}
	if !reflect.DeepEqual(dt.keys, expectedBefore) {
		t.Errorf("got %v, wanted %v", dt.keys, expectedBefore)
	}

	err := dt.RemoveColumn("test3")
	if err != nil {
		t.Errorf("got %v, wanted no error", err)
	}
	expectedAfter := []int{1, 3, 2, 0}
	if !reflect.DeepEqual(dt.keys, expectedAfter) {
		t.Errorf("got %v, wanted %v", dt.keys, expectedAfter)
	}
}

func TestRemoveColumnInKeys(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("test1", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test2", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test3", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test4", []float64{5, 4, 3, 2, 1})
	dt.AddColumn("test5", []float64{5, 4, 3, 2, 1})

	dt.SetKeys("test2", "test5", "test4", "test1")

	expectedBefore := []int{1, 4, 3, 0}
	if !reflect.DeepEqual(dt.keys, expectedBefore) {
		t.Errorf("got %v, wanted %v", dt.keys, expectedBefore)
	}

	err := dt.RemoveColumn("test2")
	if err != nil {
		t.Errorf("got %v, wanted no error", err)
	}
	expectedAfter := []int{3, 2, 0}
	if !reflect.DeepEqual(dt.keys, expectedAfter) {
		t.Errorf("got %v, wanted %v", dt.keys, expectedAfter)
	}
}

func TestRowGroupNext(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("c0", []float64{0, 1, 2, 3, 4})

	for i := 0; i < 4; i++ {
		rg := &StaticRowGroup{dt: dt, indices: make([]int, i)}
		iterations := 0
		for rg.Next() {
			iterations++
		}
		if iterations != i {
			t.Errorf("test %d: got %v, wanted %v", i, iterations, i)
		}
	}
}

func TestRowGroupValues(t *testing.T) {
	dt := &DataTable{}
	c0 := []float64{0, 1, 2, 3, 4}
	c1 := []float64{1, 2, 4, 8, 16}
	c2 := []string{"a", "b", "c", "d", "e"}
	dt.AddColumn("c0", c0)
	dt.AddColumn("c1", c1)
	dt.AddStringColumn("c2", c2)

	rg := &StaticRowGroup{dt: dt, indices: []int{0, 1, 2, 3, 4}}
	iteration := 0
	for rg.Next() {
		v0, exists := rg.FloatValue("c0")
		if !exists {
			t.Errorf("c0: iteration %d not found", iteration)
		}
		if v0 != c0[iteration] {
			t.Errorf("c0: got %v, wanted %v", v0, c0[iteration])
		}

		v1, exists := rg.FloatValue("c1")
		if !exists {
			t.Errorf("c1: iteration %d not found", iteration)
		}
		if v1 != c1[iteration] {
			t.Errorf("c1: got %v, wanted %v", v1, c1[iteration])
		}

		v2, exists := rg.StringValue("c2")
		if !exists {
			t.Errorf("c1: iteration %d not found", iteration)
		}
		if v2 != c2[iteration] {
			t.Errorf("c2: got %v, wanted %v", v2, c2[iteration])
		}

		iteration++
	}
}

func TestAggregate(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("c1", []float64{1, 1, 1, 2, 2, 3, 3, 3, 4})
	dt.AddColumn("c2", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9})
	dt.AddColumn("c3", []float64{0, 1, 0, 1, 0, 1, 0, 1, 0})
	dt.SetKeys("c1")

	testCases := []struct {
		agg      Aggregator
		indices  []int
		expected []float64
	}{
		{ // compute count of each group
			agg:      Count(),
			indices:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			expected: []float64{3, 3, 3, 2, 2, 3, 3, 3, 1},
		},

		{ // compute count of groups only for selected rows
			agg:      Count(),
			indices:  []int{1, 3, 5, 7},
			expected: []float64{math.NaN(), 1, math.NaN(), 1, math.NaN(), 2, math.NaN(), 2, math.NaN()},
		},

		{ // compute sum of each group
			agg:      Sum("c2"),
			indices:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			expected: []float64{6, 6, 6, 9, 9, 21, 21, 21, 9},
		},

		{ // compute sum of groups only for selected rows
			agg:      Sum("c2"),
			indices:  []int{1, 3, 5, 7},
			expected: []float64{math.NaN(), 2, math.NaN(), 4, math.NaN(), 14, math.NaN(), 14, math.NaN()},
		},

		{ // compute mean of each group
			agg:      Mean("c2"),
			indices:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			expected: []float64{2, 2, 2, 4.5, 4.5, 7, 7, 7, 9},
		},

		{ // compute mean of groups only for selected rows
			agg:      Mean("c2"),
			indices:  []int{1, 3, 5, 7},
			expected: []float64{math.NaN(), 2, math.NaN(), 4, math.NaN(), 7, math.NaN(), 7, math.NaN()},
		},

		{ // compute variance of each group
			agg:      Variance("c2"),
			indices:  []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			expected: []float64{1, 1, 1, 0.5, 0.5, 1, 1, 1, math.NaN()},
		},

		{ // compute variance of groups only for selected rows
			agg:      Variance("c2"),
			indices:  []int{1, 3, 5, 7},
			expected: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 2, math.NaN(), 2, math.NaN()},
		},
	}

	for i, tc := range testCases {
		aggresult := fillNaN(dt.Len())
		dt.AggregateIndexFill(aggresult, tc.agg, tc.indices)

		if !equivalentFloatSlices(aggresult, tc.expected) {
			t.Errorf("%d agg: got %v, wanted %v", i, aggresult, tc.expected)
		}
	}
}

var expectedCSV = `c1,c2
1,1
1,2
1,3
2,4
2,5
3,6
3,7
3,8
4,9
`

func TestCSV(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("c1", []float64{1, 1, 1, 2, 2, 3, 3, 3, 4})
	dt.AddColumn("c2", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9})
	buf := new(bytes.Buffer)
	dt.CSV(buf)
	if expectedCSV != buf.String() {
		t.Errorf("Expected")
		t.Errorf(expectedCSV)
		t.Errorf("Got")
		t.Errorf(buf.String())
	}
}

var benchmarkOutput interface{}

func makeFloatSlice(n int, rng *rand.Rand) []float64 {
	c := make([]float64, n)
	v := rng.Float64()
	for i := 0; i < n; i++ {
		if rng.Float64() > 0.7 {
			v = rng.Float64()
		}
		c[i] = v
	}
	return c
}

func makeTable(cols, rows int) *DataTable {
	rng := rand.New(rand.NewSource(41299))
	dt := &DataTable{}
	for i := 0; i < cols; i++ {
		dt.AddColumn(fmt.Sprintf("c%d", i), makeFloatSlice(rows, rng))
	}
	return dt
}

func BenchmarkAddColumn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dt := &DataTable{}
		dt.AddColumn("test", []float64{5, 4, 3, 2, 1})
	}
}

func BenchmarkRowSmallNumeric(b *testing.B) {
	doBenchmarkRow(makeTable(3, 100), b)
}

func BenchmarkRowMedNarrowNumeric(b *testing.B) {
	doBenchmarkRow(makeTable(3, 1000), b)
}

func BenchmarkRowMedWideNumeric(b *testing.B) {
	doBenchmarkRow(makeTable(40, 1000), b)
}

func BenchmarkRowBigNarrowNumeric(b *testing.B) {
	doBenchmarkRow(makeTable(3, 10000), b)
}

func BenchmarkRowBigWideNumeric(b *testing.B) {
	doBenchmarkRow(makeTable(40, 10000), b)
}

func doBenchmarkRow(dt *DataTable, b *testing.B) {
	b.ResetTimer()
	b.StartTimer()
	var row []interface{}
	for i := 0; i < b.N; i++ {
		row, _ = dt.Row(i % 5)
	}
	benchmarkOutput = row
}

func BenchmarkSwapSmallNumeric(b *testing.B) {
	doBenchmarkSwap(makeTable(3, 100), b)
}

func BenchmarkSwapMedNarrowNumeric(b *testing.B) {
	doBenchmarkSwap(makeTable(3, 1000), b)
}

func BenchmarkSwapMedWideNumeric(b *testing.B) {
	doBenchmarkSwap(makeTable(40, 1000), b)
}

func BenchmarkSwapBigNarrowNumeric(b *testing.B) {
	doBenchmarkSwap(makeTable(3, 10000), b)
}

func BenchmarkSwapBigWideNumeric(b *testing.B) {
	doBenchmarkSwap(makeTable(40, 10000), b)
}

func doBenchmarkSwap(dt *DataTable, b *testing.B) {
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		dt.Swap(0, 2)
	}
}

func doBenchmarkAggregator(dt *DataTable, fn Aggregator, b *testing.B) {
	rg := &StaticRowGroup{dt: dt, indices: fillSeq(dt.Len())}
	b.ResetTimer()
	b.StartTimer()

	var r float64
	for i := 0; i < b.N; i++ {
		r = fn.Aggregate(rg)
	}
	benchmarkOutput = r
}

func BenchmarkSumSmallNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 100), Sum("c0"), b)
}

func BenchmarkSumMedNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 1000), Sum("c0"), b)
}

func BenchmarkSumBigNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 10000), Sum("c0"), b)
}

func BenchmarkMeanSmallNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 100), Mean("c0"), b)
}

func BenchmarkMeanMedNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 1000), Mean("c0"), b)
}

func BenchmarkMeanBigNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 10000), Mean("c0"), b)
}

func BenchmarkVarianceSmallNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 100), Variance("c0"), b)
}

func BenchmarkVarianceMedNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 1000), Variance("c0"), b)
}

func BenchmarkVarianceBigNumeric(b *testing.B) {
	doBenchmarkAggregator(makeTable(1, 10000), Variance("c0"), b)
}

func doBenchmarkAggregate(dt *DataTable, b *testing.B) {
	col := fillNaN(dt.Len())
	fn := Sum("c0")
	indices := fillSeq(dt.Len())
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dt.AggregateIndexFill(col, fn, indices)
	}
}

func BenchmarkAggregateSmallNumeric(b *testing.B) {
	doBenchmarkAggregate(makeTable(3, 100), b)
}

func BenchmarkAggregateMedNarrowNumeric(b *testing.B) {
	doBenchmarkAggregate(makeTable(3, 1000), b)
}

func BenchmarkAggregateMedWideNumeric(b *testing.B) {
	doBenchmarkAggregate(makeTable(40, 1000), b)
}

func BenchmarkAggregateBigNarrowNumeric(b *testing.B) {
	doBenchmarkAggregate(makeTable(3, 10000), b)
}

func BenchmarkIterAggregateBigWideNumeric(b *testing.B) {
	doBenchmarkAggregate(makeTable(40, 10000), b)
}

func doBenchmarkMatches(dt *DataTable, m Matcher, b *testing.B) {
	b.ResetTimer()
	b.StartTimer()

	var r []int
	for i := 0; i < b.N; i++ {
		r = dt.Matches(m)
	}
	benchmarkOutput = r
}

func BenchmarkMatchesSmallLowNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 100), GreaterThan("c0", 0.95), b)
}

func BenchmarkMatchesSmallMedNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 100), GreaterThan("c0", 0.5), b)
}

func BenchmarkMatchesSmallHighNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 100), GreaterThan("c0", 0.05), b)
}

func BenchmarkMatchesMedLowNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 1000), GreaterThan("c0", 0.95), b)
}

func BenchmarkMatchesMedMedNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 1000), GreaterThan("c0", 0.5), b)
}

func BenchmarkMatchesMedHighNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 1000), GreaterThan("c0", 0.05), b)
}

func BenchmarkMatchesBigLowNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 10000), GreaterThan("c0", 0.95), b)
}

func BenchmarkMatchesBigMedNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 10000), GreaterThan("c0", 0.5), b)
}

func BenchmarkMatchesBigHighNumeric(b *testing.B) {
	doBenchmarkMatches(makeTable(3, 10000), GreaterThan("c0", 0.05), b)
}

func TestApplyWhere(t *testing.T) {
	dt := &DataTable{}
	dt.AddColumn("c0", []float64{
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
		1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
	})
	dt.AddStringColumn("idx", []string{
		"a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
		"a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
		"a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
		"a", "a", "a", "b", "b", "b", "b", "b", "b", "b",
		"b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
		"b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
		"c", "c", "c", "c", "c", "c", "c", "c", "d", "d",
		"d", "d", "d", "d", "d", "d", "d", "d", "d", "d",
		"d", "d", "d", "d", "d", "d", "d", "d", "d", "d",
		"d", "d", "d", "d", "d", "d", "d", "d", "d", "d",
	})
	dt.SetKeys("idx")

	// Count how many number 3's are in each group
	expected := []int{7, 5, 2, 6}
	actual := []int{}

	g := GrouperFunc(func(rg RowGroup) {
		count := 0
		for rg.Next() {
			count++
		}
		actual = append(actual, count)
	})
	matcher := MatcherFunc(func(row RowRef) bool { v, exists := row.FloatValue("c0"); return exists && v == 3.0 })

	// Record the matched rows

	dt.ApplyWhere(g, matcher)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("got %+v, wanted %+v", actual, expected)
	}
}

func doBenchmarkApplyWhere(dt *DataTable, m Matcher, b *testing.B) {
	g := GrouperFunc(func(rg RowGroup) {})
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dt.ApplyWhere(g, m)
	}
}

func BenchmarkApplyWhereSmallLowNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 100), GreaterThan("c0", 0.95), b)
}

func BenchmarkApplyWhereSmallMedNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 100), GreaterThan("c0", 0.5), b)
}

func BenchmarkApplyWhereSmallHighNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 100), GreaterThan("c0", 0.05), b)
}

func BenchmarkApplyWhereMedLowNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 1000), GreaterThan("c0", 0.95), b)
}

func BenchmarkApplyWhereMedMedNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 1000), GreaterThan("c0", 0.5), b)
}

func BenchmarkApplyWhereMedHighNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 1000), GreaterThan("c0", 0.05), b)
}

func BenchmarkApplyWhereBigLowNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 10000), GreaterThan("c0", 0.95), b)
}

func BenchmarkApplyWhereBigMedNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 10000), GreaterThan("c0", 0.5), b)
}

func BenchmarkApplyWhereBigHighNumeric(b *testing.B) {
	doBenchmarkApplyWhere(makeTable(3, 10000), GreaterThan("c0", 0.05), b)
}
