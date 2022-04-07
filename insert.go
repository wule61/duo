package duo

type InsertBuilder struct {
	Builder
	table     string
	schema    string
	columns   []string
	defaults  bool
	returning []string
	values    [][]any
}

func Insert(table string) *InsertBuilder {
	return &InsertBuilder{table: table}
}

func (i *InsertBuilder) Schema(schema string) *InsertBuilder {
	i.schema = schema
	return i
}

func (i *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	i.columns = append(i.columns, columns...)
	return i
}

func (i *InsertBuilder) Values(values ...any) *InsertBuilder {
	i.values = append(i.values, values)
	return i
}

func (i *InsertBuilder) Set(column string, v any) *InsertBuilder {
	i.columns = append(i.columns, column)
	if len(i.values) == 0 {
		i.values = append(i.values, []any{v})
	} else {
		i.values[0] = append(i.values[0], v)
	}
	return i
}

func (i *InsertBuilder) Defaulte() *InsertBuilder {
	i.defaults = true
	return i
}

func (i *InsertBuilder) Returning(columns ...string) *InsertBuilder {
	i.returning = append(i.returning, columns...)
	return i
}

func (i *InsertBuilder) writeDefault() *InsertBuilder {
	switch i.Dialect() {
	case MySQL:
		i.WriteString("()")
	case Postgres, SQLite:
		i.WriteString("DEFAULT VALUES")
	}
	return i
}

func (i *InsertBuilder) Query() (string, []any) {
	i.WriteString("INSERT INTO ")
	i.writeSchema(i.schema)
	i.Ident(i.table).Pad()
	if i.defaults && len(i.columns) == 0 {
		i.writeDefault()
	} else {
		i.WriteByte('(').IdentComma(i.columns...).WriteByte(')')
		i.WriteString(" VALUES ")
		for j, v := range i.values {
			if j > 0 {
				i.Comma()
			}
			i.WriteByte('(').Args(v...).WriteByte(')')
		}
	}
	if len(i.returning) > 0 && !i.mysql() {
		i.WriteString(" RETURNING ")
		i.IdentComma(i.returning...)
	}

	return i.String(), i.args
}
