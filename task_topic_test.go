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
	for i := 0; i < 10000; i++ {
		topic.Push([]byte("hello world"))
	}
	t.Log(topic.TaskQueue.Length())
	t.Log(topic.RetryQueue.Length())
	for i := 0; i < 10000; i++ {
		key, d, err := topic.Pop()
		if err != nil && err != ds.ErrEmpty {
			t.Error(err)
		}
		if err = topic.Confirm(key); err != nil {
			t.Error(err)
		}
		if i == 5000 {
			t.Log(topic.TaskQueue.Length())
			t.Log(key)
			t.Log(string(d))
		}
	}
	t.Log(topic.TaskQueue.Length())
	t.Log(topic.RetryQueue.Length())
}
