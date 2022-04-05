package duo

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestScanSlice(t *testing.T) {
	mock := sqlmock.NewRows([]string{"name"}).
		AddRow("foo").
		AddRow("bar")
	var strArr []string
	strArr, err := ScanSlice[string](toRows(mock))
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo", "bar"}, strArr)

	mock = sqlmock.NewRows([]string{"age"}).AddRow(1).AddRow(2)
	var intArr []int
	intArr, err = ScanSlice[int](toRows(mock))
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2}, intArr)

	mock = sqlmock.NewRows([]string{"name", "COUNT(*)"}).AddRow("foo", 1).AddRow("bar", 2)
	type NameAge struct {
		Name  string
		Count int
	}

	NameAgeArr, err := ScanSlice[NameAge](toRows(mock))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(NameAgeArr))
	assert.Equal(t, "foo", NameAgeArr[0].Name)
	assert.Equal(t, "bar", NameAgeArr[1].Name)

	mock = sqlmock.NewRows([]string{"name", "COUNT(*)"}).AddRow("foo", 2).AddRow("bar", 3)
	NameAgePtrArr, err := ScanSlice[*NameAge](toRows(mock))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(NameAgePtrArr))
	assert.Equal(t, "foo", NameAgePtrArr[0].Name)
	assert.Equal(t, "bar", NameAgePtrArr[1].Name)

}

func toRows(mrows *sqlmock.Rows) *sql.Rows {
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("").WillReturnRows(mrows)
	rows, _ := db.Query("")
	return rows
}
