package duo

type Predicate struct {
	Builder
	depth int
	fns   []func(*Builder)
}

func P(fns ...func(*Builder)) *Predicate {
	return &Predicate{fns: fns}
}

func Or(preds ...*Predicate) *Predicate {
	p := P()
	return p.Append(func(b *Builder) {
		p.mayWrap(preds, b, "OR")
	})
}

func False() *Predicate {
	return P().False()
}

func (p *Predicate) False() *Predicate {
	return p.Append(func(b *Builder) {
		b.WriteString("FALSE")
	})
}

func Not(pred *Predicate) *Predicate {
	return P().Not().Append(func(b *Builder) {
		b.Nested(func(b *Builder) {
			b.Join(pred)
		})
	})
}

func (p *Predicate) Not() *Predicate {
	return p.Append(func(b *Builder) {
		b.WriteString("NOT")
	})
}

func ColumnsOp(col1, col2 string, op Op) *Predicate {
	return P().ColumnsOp(col1, col2, op)
}

func (p *Predicate) ColumnsOp(col1, col2 string, op Op) *Predicate {
	return p.Append(func(b *Builder) {
		b.isIdent(col1)
		b.WriteOp(op)
		b.isIdent(col2)
	})
}

func And(preds ...*Predicate) *Predicate {
	p := P()
	return p.Append(func(b *Builder) {
		p.mayWrap(preds, b, "AND")
	})
}

func (p *Predicate) EQ(col string, arg any) *Predicate {
	return p.op(col, OpEQ, arg)
}

func ColumnsEQ(col1, col2 string) *Predicate {
	return ColumnsOp(col1, col2, OpEQ)
}

func (p *Predicate) ColumnsEQ(col1, col2 string) *Predicate {
	return p.ColumnsOp(col1, col2, OpEQ)
}

func (p *Predicate) op(col string, op Op, arg any) *Predicate {
	return p.Append(func(b *Builder) {
		b.Ident(col)
		b.WriteOp(op)
		p.arg(b, arg)
	})
}

func NEQ(col string, arg any) *Predicate {
	return P().NEQ(col, arg)
}

func (p *Predicate) NEQ(col string, arg any) *Predicate {
	return p.op(col, OpNEQ, arg)
}

func (p *Predicate) arg(b *Builder, a any) {
	// todo
	b.Arg(a)
}

func (p *Predicate) mayWrap(preds []*Predicate, b *Builder, op string) {
	switch n := len(preds); {
	case n == 1:
		b.Join(preds[0])
		return
	case n > 1 && p.depth != 0:
		b.WriteByte('(')
		defer b.WriteByte(')')
	}
	for i := range preds {
		preds[i].depth = p.depth + 1
		if i > 0 {
			b.WriteByte(' ')
			b.WriteString(op)
			b.WriteByte(' ')
		}
		if len(preds[i].fns) > 1 {
			b.Nested(func(b *Builder) {
				b.Join(preds[i])
			})
		} else {
			b.Join(preds[i])
		}
	}
}

func (p *Predicate) Append(f func(*Builder)) *Predicate {
	p.fns = append(p.fns, f)
	return p
}
