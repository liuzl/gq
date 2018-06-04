package gq

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/liuzl/ds"
	"github.com/liuzl/goutil"
	"github.com/liuzl/store"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type TaskTopic struct {
	TaskQueue    *ds.Queue
	RetryQueue   *ds.Queue
	RunningStore *store.LevelStore

	dir     string
	timeout int64
	wg      *sync.WaitGroup
	exitCh  chan bool
}

func NewTaskTopic(dir string, wg *sync.WaitGroup) (*TaskTopic, error) {
	t := &TaskTopic{dir: dir, timeout: 300, wg: wg, exitCh: make(chan bool)}
	var err error
	queueDir := filepath.Join(dir, "queue")
	if t.TaskQueue, err = ds.OpenQueue(queueDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(dir, "retry_queue")
	if t.RetryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(dir, "running")
	if t.RunningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	t.wg.Add(1)
	go t.retry()

	return t, nil
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

func (t *TaskTopic) pop(q *ds.Queue) (string, []byte, error) {
	item, err := q.Dequeue()
	if err != nil {
		return "", nil, err
	}
	now := time.Now().Unix()
	key := goutil.TimeStr(now+t.timeout) + ":" + goutil.ContentMD5(item.Value)
	if err = t.addToRunning(key, item.Value); err != nil {
		return "", nil, err
	}
	return key, item.Value, nil
}

func (t *TaskTopic) Pop() (string, []byte, error) {
	if t.RetryQueue != nil && t.RetryQueue.Length() > 0 {
		return t.pop(t.RetryQueue)
	}
	if t.TaskQueue != nil && t.TaskQueue.Length() > 0 {
		return t.pop(t.TaskQueue)
	}
	return "", nil, fmt.Errorf("Queue is empty")
}

func (t *TaskTopic) Confirm(key string) error {
	if t.RunningStore == nil {
		return fmt.Errorf("RunningStore is nil")
	}
	return t.RunningStore.Delete(key)
}

func (t *TaskTopic) Close() {
	if t.exitCh != nil {
		t.exitCh <- true
	}
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

func (t *TaskTopic) addToRunning(key string, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if t.RunningStore == nil {
		return fmt.Errorf("RunningStore is nil")
	}
	return t.RunningStore.Put(key, value)
}

func (t *TaskTopic) retry() {
	defer t.wg.Done()
	for {
		select {
		case <-t.exitCh:
			return
		default:
			now := time.Now().Format("20060102030405")
			t.RunningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					if _, err := t.RetryQueue.Enqueue(value); err != nil {
						return false, err
					}
					return true, nil
				})
			goutil.Sleep(5*time.Second, t.exitCh)
		}
	}
}
