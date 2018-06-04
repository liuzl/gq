package gq

import (
	"sync"
	"testing"
	//	"github.com/liuzl/goutil"
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
	t2 := NewInstructionTopic("phone", []byte("Get phone"))

	item1 := &TopicItem{"task", topic}
	item2 := &TopicItem{"instruction", t2}
	t.Log(item1)
	t.Log(item2)
}
