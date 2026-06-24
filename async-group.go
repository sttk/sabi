// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync"

	"github.com/sttk/errs"
)

// IndexedErr represents an error that occurred during an asynchronous task,
// paired with the index of the task to identify which execution failed.
//
// It is a struct that associates an error value (errs.Err) with an integer index.
//
// In concurrent or batch processing, multiple tasks are executed asynchronously.
// If one or more tasks fail, it is crucial to know which task generated which error.
// IndexedErr provides a mapping between the task (via its index) and the resulting error,
// allowing callers to pinpoint the source of failure (e.g., matching the error to a
// specific data source or connection index).
//
// Typically, you receive a slice of IndexedErr created by AsyncGroup. You can
// iterate over this slice and use the Index field to retrieve the original context
// or metadata of the failed task, and the Err field to handle the error itself.
type IndexedErr struct {
	// Index is the task identifier or sequence number (typically the index in a slice
	// of tasks or resources) associated with the failed asynchronous execution.
	// This index is determined by the order in which they were added to AsyncGroup.
	Index int

	// Err is the error returned by the asynchronous task. It uses the errs.Err type
	// from the "github.com/sttk/errs" package to represent the failure details.
	Err errs.Err
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
	ierrs  []IndexedErr
	_index int
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
	go func(index int) {
		defer ag.wg.Done()
		err := fn()
		if err.IsNotOk() {
			ag.mutex.Lock()
			defer ag.mutex.Unlock()
			ag.addErr(index, err)
		}
	}(ag._index)
}

func (ag *AsyncGroup) addErr(index int, err errs.Err) {
	ag.ierrs = append(ag.ierrs, IndexedErr{Index: index, Err: err})
}

func (ag *AsyncGroup) join() []IndexedErr {
	ag.wg.Wait()
	return ag.ierrs
}
