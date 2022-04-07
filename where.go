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
