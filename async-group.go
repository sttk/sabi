// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync"

	"github.com/sttk/errs"
)

type ErrEntry struct {
	Index int

	Name string

	Err errs.Err
}

type AsyncGroup struct {
	errors []ErrEntry
	_index int
	_name  string
	wg     sync.WaitGroup
	mutex  sync.Mutex
}

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
