// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type DataConn interface {
	Commit(ag *AsyncGroup) errs.Err
	PreCommit(ag *AsyncGroup) errs.Err
	PostCommit(ag *AsyncGroup)
	ShouldForceBack() bool
	Rollback(ag *AsyncGroup)
	ForceBack(ag *AsyncGroup)
	Close()
}

type dataConnContainer struct {
	prev *dataConnContainer
	next *dataConnContainer
	name string
	conn DataConn
}

type dataConnList struct {
	head *dataConnContainer
	last *dataConnContainer
}

func (list *dataConnList) appendContainer(ptr *dataConnContainer) {
	ptr.next = nil

	if list.last == nil {
		list.head = ptr
		list.last = ptr
		ptr.prev = nil
	} else {
		list.last.next = ptr
		ptr.prev = list.last
		list.last = ptr
	}
}

func (list *dataConnList) closeDataConns() {
	ptr := list.last
	for ptr != nil {
		ptr.conn.Close()
		ptr = ptr.prev
	}
	list.head = nil
	list.last = nil
}
