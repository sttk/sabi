// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync"

	"github.com/sttk/errs"
)

type IndexedErr struct {
	Index int
	Err   errs.Err
}

type AsyncGroup struct {
	ierrs  []IndexedErr
	_index int
	wg     sync.WaitGroup
	mutex  sync.Mutex
}

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
