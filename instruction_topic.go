package gq

type InstructionTopic struct {
	Name string `json:"name"`
	Jobs string `json:"jobs"`
}

func NewInstructionTopic(name string, jobs string) *InstructionTopic {
	return &InstructionTopic{name, jobs}
}

func (i *InstructionTopic) Type() string { return "INSTRUCTION" }

func (i *InstructionTopic) Push([]byte) error { return nil }

func (i *InstructionTopic) Pop() (string, []byte, error) {
	return "", []byte(i.Jobs), nil
}

func (i *InstructionTopic) Confirm(string) error { return nil }

func (i *InstructionTopic) Close() {}
