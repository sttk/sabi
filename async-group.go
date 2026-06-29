// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync"

	"github.com/sttk/errs"
)

type ErrEntry struct {
	Index int
	Name  string
	Err   errs.Err
}

// AsyncGroup coordinates the execution of multiple asynchronous tasks and
// collects any errors they return, associating each error with the index of the task.
//
// It is a synchronization structure similar to sync.WaitGroup but specialized for
// tracking errors returned by asynchronous functions. It manages a wait group, a mutex
// for thread safety, and an internal list of indexed errors.
//
// When initializing or operating multiple independent resources concurrently (such as setting up
// multiple data sources or committing transactions across multiple connections), AsyncGroup
// allows executing these operations in separate goroutines while safely aggregating all errors
// that occur, along with their order or resource index.
type AsyncGroup struct {
	errors []ErrEntry
	_index int
	_name  string
	wg     sync.WaitGroup
	mutex  sync.Mutex
}

// Add starts the execution of the given function in a new goroutine and
// registers it with the AsyncGroup.
//
// It increments the internal WaitGroup counter and spawns a new goroutine that runs
// the provided function fn. If the function completes and returns a non-OK error,
// the error is safely recorded along with the current index which indicates the order
// in the AsyncGroup.
// Once fn completes, the WaitGroup counter is decremented.
func (ag *AsyncGroup) Add(fn func() errs.Err) {
	ag.wg.Add(1)
	go func(index int, name string) {
		defer ag.wg.Done()
		err := fn()
		if err.IsNotOk() {
			ag.mutex.Lock()
			defer ag.mutex.Unlock()
			ag.addErr(index, name, err)
		}
	}(ag._index, ag._name)
}

func (ag *AsyncGroup) addErr(index int, name string, err errs.Err) {
	ag.errors = append(ag.errors, ErrEntry{Index: index, Name: name, Err: err})
}

func (ag *AsyncGroup) join() []ErrEntry {
	ag.wg.Wait()
	return ag.errors
}
