package gq

import (
	"sync"
	"testing"

	"github.com/liuzl/ds"
)

func TestTaskTopic(t *testing.T) {
	var dir = "crawl"
	var wg sync.WaitGroup
	topic, err := NewTaskTopic(dir, &wg)
	if err != nil {
		t.Error(err)
	}
	defer topic.Close()
	topic.Push([]byte("hello world"))
	key, d, err := topic.Pop()
	if err != nil && err != ds.ErrEmpty {
		t.Error(err)
	}
	if err = topic.Confirm(key); err != nil {
		t.Error(err)
	}
	t.Log(key)
	t.Log(string(d))
}
