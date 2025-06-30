// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync"

	"github.com/sttk/errs"
)

type errEntry struct {
	name string
	err  errs.Err
	next *errEntry
}

// AsyncGroup allows for the asynchronous execution of multiple functions and
// waits for all of them to complete. Functions are added using the Add
// method and are then run concurrently in separate goroutines.
//
// The AsyncGroup ensures that all added tasks finish before proceeding,
// collecting any errors that occur during their execution.
type AsyncGroup struct {
	errHead *errEntry
	errLast *errEntry
	wg      sync.WaitGroup
	mutex   sync.Mutex
	name    string
}

// Add an asynchronous function to the group, and starts it in a new goroutine.
//
// This given function is executed asynchronously with other similarly given functions,
// awaiting completion and collecting errors internally.
func (ag *AsyncGroup) Add(fn func() errs.Err) {
	ag.wg.Add(1)
	go func(name string) {
		defer ag.wg.Done()
		err := fn()
		if err.IsNotOk() {
			ag.mutex.Lock()
			defer ag.mutex.Unlock()
			ag.addErr(name, err)
		}
	}(ag.name)
}

func (ag *AsyncGroup) addErr(name string, err errs.Err) {
	ent := &errEntry{name: name, err: err}
	if ag.errLast == nil {
		ag.errHead = ent
		ag.errLast = ent
	} else {
		ag.errLast.next = ent
		ag.errLast = ent
	}
}

func (ag *AsyncGroup) joinAndPutErrorsInto(errMap map[string]errs.Err) {
	ag.wg.Wait()

	for ent := ag.errHead; ent != nil; ent = ent.next {
		errMap[ent.name] = ent.err
	}
}

func (ag *AsyncGroup) joinAndIgnoreErrors() {
	ag.wg.Wait()
}
