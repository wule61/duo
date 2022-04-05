package duo

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/modern-go/reflect2"
)

func ScanOne(rows ColumnScanner, v any) error {
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sql/scan: failed getting column names: %w", err)
	}

	if n := len(columns); n != 1 {
		return fmt.Errorf("sql/scan: unexpected number of columns: %d", n)
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	if err := rows.Scan(v); err != nil {
		return err
	}

	if rows.Next() {
		return fmt.Errorf("sql/scan: expect exactly one row in result set")
	}
	return nil
}

func Scan[T any](rows ColumnScanner) (T, error) {
	var n T
	if err := ScanOne(rows, &n); err != nil {
		return n, err
	}
	return n, nil
}

func ScanValue(rows ColumnScanner) (driver.Value, error) {
	var v driver.Value
	if err := ScanOne(rows, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func ScanSlice[T any](rows ColumnScanner) ([]T, error) {

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var t T
	e := reflect.ValueOf(reflect2.TypeOf(t).New()).Elem()
	scan, err := scanType(e.Type(), columns)
	if err != nil {
		return nil, err
	}
	if n, m := len(columns), len(scan.columns); n > m {
		return nil, fmt.Errorf("sql/scan: columns do not match (%d > %d)", n, m)
	}

	var res []T
	for rows.Next() {
		values := scan.values()
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("sql/scan: failed scanning rows: %w", err)
		}
		res = append(res, scan.value(values...).Interface().(T))
	}

	return res, rows.Err()
}

type rowScan struct {
	// column types of a row.
	columns []reflect.Type
	// value functions that converts the row columns (result) to a reflect.Value.
	value func(v ...any) reflect.Value
}

func (r *rowScan) values() []any {
	values := make([]any, len(r.columns))
	for i := range r.columns {
		values[i] = reflect.New(r.columns[i]).Interface()
	}
	return values
}

// scanType returns rowScan for the given reflect.Type.
func scanType(typ reflect.Type, columns []string) (*rowScan, error) {
	switch k := typ.Kind(); {
	case assignable(typ):
		return &rowScan{
			columns: []reflect.Type{typ},
			value: func(v ...any) reflect.Value {
				return reflect.Indirect(reflect.ValueOf(v[0]))
			},
		}, nil
	case k == reflect.Ptr:
		return scanPtr(typ, columns)
	case k == reflect.Struct:
		return scanStruct(typ, columns)
	default:
		return nil, fmt.Errorf("sql/scan: unsupported type ([]%s)", k)
	}
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func assignable(typ reflect.Type) bool {
	switch k := typ.Kind(); {
	case typ.Implements(scannerType):
	case k == reflect.Interface && typ.NumMethod() == 0:
	case k == reflect.String || k >= reflect.Bool && k <= reflect.Float64:
	case (k == reflect.Slice || k == reflect.Array) && typ.Elem().Kind() == reflect.Uint8:
	default:
		return false
	}
	return true
}

func scanPtr(typ reflect.Type, columns []string) (*rowScan, error) {
	typ = typ.Elem()
	scan, err := scanType(typ, columns)
	if err != nil {
		return nil, err
	}
	wrap := scan.value
	scan.value = func(vs ...any) reflect.Value {
		v := wrap(vs...)
		pt := reflect.PtrTo(v.Type())
		pv := reflect.New(pt.Elem())
		pv.Elem().Set(v)
		return pv
	}
	return scan, nil
}

func scanStruct(typ reflect.Type, columns []string) (*rowScan, error) {
	var (
		scan  = &rowScan{}
		idxs  = make([][]int, 0, typ.NumField())
		names = make(map[string][]int, typ.NumField())
	)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		// Skip unexported fields.
		if f.PkgPath != "" {
			continue
		}
		// Support 1-level embedding to accepts types as `type T struct {ent.T; V int}`.
		if typ := f.Type; f.Anonymous && typ.Kind() == reflect.Struct {
			for j := 0; j < typ.NumField(); j++ {
				names[columnName(typ.Field(j))] = []int{i, j}
			}
			continue
		}
		names[columnName(f)] = []int{i}
	}
	for _, c := range columns {
		// Normalize columns if necessary, for example: COUNT(*) => count.
		name := strings.ToLower(strings.Split(c, "(")[0])
		idx, ok := names[name]
		if !ok {
			return nil, fmt.Errorf("sql/scan: missing struct field for column: %s (%s)", c, name)
		}
		idxs = append(idxs, idx)
		rtype := typ.Field(idx[0]).Type
		if len(idx) > 1 {
			rtype = rtype.Field(idx[1]).Type
		}
		if !nillable(rtype) {
			// Create a pointer to the actual reflect
			// types to accept optional struct fields.
			rtype = reflect.PtrTo(rtype)
		}
		scan.columns = append(scan.columns, rtype)
	}
	scan.value = func(vs ...any) reflect.Value {
		st := reflect.New(typ).Elem()
		for i, v := range vs {
			rv := reflect.Indirect(reflect.ValueOf(v))
			if rv.IsNil() {
				continue
			}
			idx := idxs[i]
			rvalue := st.Field(idx[0])
			if len(idx) > 1 {
				rvalue = rvalue.Field(idx[1])
			}
			if !nillable(rvalue.Type()) {
				rv = reflect.Indirect(rv)
			}
			rvalue.Set(rv)
		}
		return st
	}
	return scan, nil
}

func columnName(f reflect.StructField) string {
	name := strings.ToLower(f.Name)
	if tag, ok := f.Tag.Lookup("sql"); ok {
		name = tag
	} else if tag, ok := f.Tag.Lookup("json"); ok {
		name = strings.Split(tag, ",")[0]
	}
	return name
}

func nillable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Slice, reflect.Map, reflect.Ptr, reflect.UnsafePointer:
		return true
	}
	return false
}
