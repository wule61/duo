package duo

import (
	"fmt"
	"strconv"
	"strings"
)

type Table interface {
	TableName() string
}

type Querier interface {
	Query() (string, []any)
}

const (
	MySQL    = "mysql"
	SQLite   = "sqlite3"
	Postgres = "postgres"
)

type Builder struct {
	sb        *strings.Builder
	dialect   string
	args      []any
	total     int
	errs      []error
	qualifier string
}

func (b *Builder) Quote(ident string) string {
	quote := "`"
	switch {
	case b.postgres():
		if strings.ContainsAny(ident, "`") {
			return strings.ReplaceAll(ident, "`", `"`)
		}
		quote = `"`
	case b.dialect == "" && strings.ContainsAny(ident, "`\""):
		return ident
	}

	return quote + ident + quote
}

func (b *Builder) Ident(s string) *Builder {

	switch {
	case len(s) == 0:
	case s != "*" && !b.isIdent(s) && !isFunc(s) && !isModifier(s):
		if b.qualifier != "" {
			b.WriteString(b.Quote(b.qualifier)).WriteByte('.')
		}
		b.WriteString(b.Quote(s))
	case (isFunc(s) || isModifier(s)) && b.postgres():
		b.WriteString(strings.ReplaceAll(s, "`", `"`))
	default:
		b.WriteString(s)
	}

	return b
}

func (b *Builder) IdentComma(s ...string) *Builder {
	for i := range s {
		if i > 0 {
			b.Comma()
		}
		b.Ident(s[i])
	}
	return b
}

func (b *Builder) Arg(a any) *Builder {
	b.total++
	b.args = append(b.args, a)

	param := "?"
	if b.postgres() {
		param = "$" + strconv.Itoa(b.total)
	}

	b.WriteString(param)
	return b
}

func (b *Builder) Args(a ...any) *Builder {
	for i := range a {
		if i > 0 {
			b.Comma()
		}
		b.Arg(a[i])
	}
	return b
}

func (b *Builder) Comma() *Builder {
	return b.WriteString(", ")
}

func (b *Builder) Pad() *Builder {
	return b.WriteByte(' ')
}

func (b *Builder) WriteString(s string) *Builder {
	if b.sb == nil {
		b.sb = &strings.Builder{}
	}
	b.sb.WriteString(s)
	return b
}

func (b *Builder) WriteByte(c byte) *Builder {
	if b.sb == nil {
		b.sb = &strings.Builder{}
	}
	b.sb.WriteByte(c)
	return b
}

func (b *Builder) writeSchema(schema string) {
	if schema != "" && b.dialect != SQLite {
		b.Ident(schema).WriteByte('.')
	}
}

func (b *Builder) String() string {
	if b.sb == nil {
		return ""
	}
	return b.sb.String()
}

// An Op represents an operator.
type Op int

const (
	// Predicate operators.
	OpEQ      Op = iota // =
	OpNEQ               // <>
	OpGT                // >
	OpGTE               // >=
	OpLT                // <
	OpLTE               // <=
	OpIn                // IN
	OpNotIn             // NOT IN
	OpLike              // LIKE
	OpIsNull            // IS NULL
	OpNotNull           // IS NOT NULL

	// Arithmetic operators.
	OpAdd // +
	OpSub // -
	OpMul // *
	OpDiv // / (Quotient)
	OpMod // % (Reminder)
)

var ops = [...]string{
	OpEQ:      "=",
	OpNEQ:     "<>",
	OpGT:      ">",
	OpGTE:     ">=",
	OpLT:      "<",
	OpLTE:     "<=",
	OpIn:      "IN",
	OpNotIn:   "NOT IN",
	OpLike:    "LIKE",
	OpIsNull:  "IS NULL",
	OpNotNull: "IS NOT NULL",
	OpAdd:     "+",
	OpSub:     "-",
	OpMul:     "*",
	OpDiv:     "/",
	OpMod:     "%",
}

func (b *Builder) WriteOp(op Op) *Builder {
	switch {
	case op >= OpEQ && op <= OpLike || op >= OpAdd && op <= OpMod:
		b.Pad().WriteString(ops[op]).Pad()
	case op == OpIsNull || op == OpNotNull:
		b.Pad().WriteString(ops[op])
	default:
		panic(fmt.Sprintf("invalid op %d", op))
	}
	return b
}

func (b *Builder) isIdent(s string) bool {

	switch {
	case b.postgres():
		return strings.ContainsAny(s, `"`)
	default:
		return strings.ContainsAny(s, "`")
	}
}

func (b *Builder) AddError(err error) *Builder {
	if err != nil {
		b.errs = append(b.errs, err)
	}
	return b
}

type QuerierErr interface {
	Err() error
}

func (b *Builder) Err() error {
	if len(b.errs) == 0 {
		return nil
	}

	errb := strings.Builder{}
	for i := range b.errs {
		if i > 0 {
			errb.WriteString("; ")
		}
		errb.WriteString(b.errs[i].Error())
	}

	return fmt.Errorf(errb.String())
}

func (b *Builder) Dialect() string {
	return b.dialect
}

func isFunc(s string) bool {
	return strings.ContainsAny(s, "(") && strings.ContainsAny(s, ")")
}

func isModifier(s string) bool {
	for _, m := range [...]string{"DISTINCT", "ALL", "WITH ROLLUP"} {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

func (b *Builder) postgres() bool {
	return b.dialect == Postgres
}

func (b *Builder) mysql() bool {
	return b.dialect == MySQL
}

func (b *Builder) Join(qs ...Querier) *Builder {
	return b.join(qs, "")
}

func (b *Builder) JoinComma(qs ...Querier) *Builder {
	return b.join(qs, ", ")
}

func (b *Builder) join(qs []Querier, spe string) *Builder {
	for i, q := range qs {
		if i > 0 {
			b.WriteString(spe)
		}

		if st, ok := q.(state); ok {
			st.SetDialect(b.dialect)
			st.SetTotal(b.total)
		}

		query, args := q.Query()
		b.WriteString(query)
		b.args = append(b.args, args...)
		b.total += len(args)

		if qe, ok := q.(QuerierErr); ok {
			if err := qe.Err(); err != nil {
				b.AddError(err)
			}
		}
	}

	return b
}

func (b *Builder) Nested(f func(*Builder)) *Builder {
	nb := &Builder{dialect: b.dialect, total: b.total, sb: &strings.Builder{}}
	nb.WriteByte('(')
	f(nb)
	nb.WriteByte(')')
	b.WriteString(nb.String())
	b.args = append(b.args, nb.args...)
	b.total = nb.total
	return b
}

func (b *Builder) Query() (string, []any) {
	return b.String(), b.args
}

type state interface {
	Dialect() string
	SetDialect(string)
	Total() int
	SetTotal(int)
}
