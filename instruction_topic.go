package gq

type InstructionTopic struct {
	Name   string `json:"name"`
	Recipe []byte `json:"recipe"`
}

func NewInstructionTopic(name string, recipe []byte) *InstructionTopic {
	return &InstructionTopic{name, recipe}
}

func (i *InstructionTopic) Type() string { return "INSTRUCTION" }

func (i *InstructionTopic) Push([]byte) error { return nil }

func (i *InstructionTopic) Pop() (string, []byte, error) {
	return "", i.Recipe, nil
}

func (i *InstructionTopic) Confirm(string) error { return nil }

func (i *InstructionTopic) Close() {}
