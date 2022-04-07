package duo

type DeleteBuilder struct {
	Builder
	table  string
	schema string
	where  *Predicate
}
