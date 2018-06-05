package gq

import (
	"fmt"
	"sync"
	"testing"

	"github.com/liuzl/goutil"
)

type TopicItem struct {
	Type string
	Item Topic
}

func TestTopics(t *testing.T) {
	var wg sync.WaitGroup
	topic, err := NewTaskTopic("task", "crawl", &wg)
	if err != nil {
		t.Error(err)
	}
	t2 := NewInstructionTopic("mobile", "ip;loc")

	item1 := &TopicItem{"task", topic}
	item2 := &TopicItem{"instruction", t2}
	fmt.Printf("%+v\n", item1)
	fmt.Printf("%+v\n", item2)
	i1, err := goutil.JsonMarshalIndent(item1, "", "  ")
	if err != nil {
		t.Error(err)
	}
	t.Log(string(i1))

	i2, err := goutil.JsonMarshalIndent(item2, "", "  ")
	if err != nil {
		t.Error(err)
	}
	t.Log(string(i2))
}
