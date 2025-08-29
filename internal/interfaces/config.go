package interfaces

type Config interface {
	Load()
	SetDefaults()
	Validate() error
}
