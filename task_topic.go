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
	Name    string `json:"name"`
	Dir     string `json:dir`
	Timeout int64  `json:timeout`

	queue        *ds.Queue
	retryQueue   *ds.Queue
	runningStore *store.LevelStore
	wg           *sync.WaitGroup
	exit         chan bool
}

func NewTaskTopic(name, dir string, wg *sync.WaitGroup) (*TaskTopic, error) {
	t := &TaskTopic{Name: name, Dir: dir, Timeout: 300, wg: wg, exit: make(chan bool)}
	var err error
	queueDir := filepath.Join(dir, name, "queue")
	if t.queue, err = ds.OpenQueue(queueDir); err != nil {
		return nil, err
	}
	retryDir := filepath.Join(dir, name, "retry_queue")
	if t.retryQueue, err = ds.OpenQueue(retryDir); err != nil {
		return nil, err
	}
	storeDir := filepath.Join(dir, name, "running")
	if t.runningStore, err = store.NewLevelStore(storeDir); err != nil {
		return nil, err
	}

	t.wg.Add(1)
	go t.retry()

	return t, nil
}

func (t *TaskTopic) Type() string { return "TASK" }

func (t *TaskTopic) Push(data []byte) error {
	if t.queue != nil {
		_, err := t.queue.Enqueue(data)
		return err
	}
	return fmt.Errorf("queue is nil")
}

func (t *TaskTopic) pop(q *ds.Queue) (string, []byte, error) {
	item, err := q.Dequeue()
	if err != nil {
		return "", nil, err
	}
	now := time.Now().Unix()
	key := goutil.TimeStr(now+t.Timeout) + ":" + goutil.ContentMD5(item.Value)
	if err = t.addToRunning(key, item.Value); err != nil {
		return "", nil, err
	}
	return key, item.Value, nil
}

func (t *TaskTopic) Pop() (string, []byte, error) {
	if t.retryQueue != nil && t.retryQueue.Length() > 0 {
		return t.pop(t.retryQueue)
	}
	if t.queue != nil && t.queue.Length() > 0 {
		return t.pop(t.queue)
	}
	return "", nil, fmt.Errorf("Queue is empty")
}

func (t *TaskTopic) Confirm(key string) error {
	if t.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return t.runningStore.Delete(key)
}

func (t *TaskTopic) Close() {
	if t.exit != nil {
		t.exit <- true
	}
	if t.queue != nil {
		t.queue.Close()
	}
	if t.retryQueue != nil {
		t.retryQueue.Close()
	}
	if t.runningStore != nil {
		t.runningStore.Close()
	}
}

func (t *TaskTopic) addToRunning(key string, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("empty value")
	}
	if t.runningStore == nil {
		return fmt.Errorf("runningStore is nil")
	}
	return t.runningStore.Put(key, value)
}

func (t *TaskTopic) retry() {
	defer t.wg.Done()
	for {
		select {
		case <-t.exit:
			return
		default:
			now := time.Now().Format("20060102030405")
			t.runningStore.ForEach(&util.Range{Limit: []byte(now)},
				func(key, value []byte) (bool, error) {
					if _, err := t.retryQueue.Enqueue(value); err != nil {
						return false, err
					}
					return true, nil
				})
			goutil.Sleep(5*time.Second, t.exit)
		}
	}
}
