package gq

type Topic interface {
	Type() string
	Push([]byte) error
	Pop() (string, []byte, error)
	Confirm(string) error
	Close()
}
