package gq

import (
	"fmt"
	"path/filepath"

	"github.com/liuzl/ds"
	"github.com/liuzl/store"
)

type TaskTopic struct {
	dir          string
	name         string
	TaskQueue    *ds.Queue
	RetryQueue   *ds.Queue
	RunningStore *store.LevelStore
}

func NewTaskTopic(dir, name string) (*TaskTopic, error) {
	t := &TaskTopic{dir: dir, name: name}
	var err error
	queueDir := filepath.Join(dir, name, "queue")
	if t.TaskQueue, err = ds.OpenQueue(queueDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(dir, name, "retry_queue")
	if t.RetryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}

	storeDir := filepath.Join(dir, name, "running")
	if t.RunningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}
	// TODO deal with timeout tasks
	return t, nil
}

func (t *TaskTopic) Name() string {
	return t.name
}

func (t *TaskTopic) Type() string {
	return "TASK"
}

func (t *TaskTopic) Push(data []byte) error {
	if t.TaskQueue != nil {
		_, err := t.TaskQueue.Enqueue(data)
		return err
	}
	return fmt.Errorf("TaskQueue is nil")
}

func (t *TaskTopic) Pop() ([]byte, error) {
	// TODO pop from retry queue first
	if t.TaskQueue != nil {
		item, err := t.TaskQueue.Dequeue()
		if err != nil {
			return nil, err
		}
		// TODO add to running store
		return item.Value, nil
	}
	return nil, fmt.Errorf("TaskQueue is nil")
}

func (t *TaskTopic) Close() {
	if t.TaskQueue != nil {
		t.TaskQueue.Close()
	}
	if t.RetryQueue != nil {
		t.RetryQueue.Close()
	}
	if t.RunningStore != nil {
		t.RunningStore.Close()
	}
}
