package gq

type Topic interface {
	Type() string
	Push(data []byte) error
	Pop() (string, []byte, error)
	Confirm(string) error
	Close()
}
