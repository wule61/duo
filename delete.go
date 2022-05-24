package duo

type DeleteBuilder struct {
	Builder
	table  string
	schema string
	where  *Predicate
}

type DB[T Table] struct {
	Data T
}

type User struct {
	Name string
	Age  int
}

func (User) TableName() string {
	return "users"
}

type Product struct {
	ID int
}

func (Product) TableName() string {
	return "products"
}

func Create[T Table]() DB[T] {
	var t T
	de := DB[T]{Data: t}
	return de
}

func test() {

	user := Create[User]()
	user.Data.Name = "jack"
	user.Data.Age = 23

	product := Create[Product]()

	product.Data.ID = 12
	
}
