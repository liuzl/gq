package gq

type Topic interface {
	Name() string
	Type() string
	Push(data []byte) error
	Pop() ([]byte, error)
	Close()
}
