// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type DataSrc interface {
	Setup(ag *AsyncGroup) errs.Err
	Close()
	CreateDataConn() (DataConn, errs.Err)
}

type dataSrcContainer struct {
	prev  *dataSrcContainer
	next  *dataSrcContainer
	local bool
	name  string
	ds    DataSrc
}

type dataSrcList struct {
	notSetupHead *dataSrcContainer
	notSetupLast *dataSrcContainer
	didSetupHead *dataSrcContainer
	didSetupLast *dataSrcContainer
	local        bool
}

func (list *dataSrcList) appendContainerPtrNotSetup(ptr *dataSrcContainer) {
	ptr.next = nil

	if list.notSetupLast == nil {
		list.notSetupHead = ptr
		list.notSetupLast = ptr
		ptr.prev = nil
	} else {
		list.notSetupLast.next = ptr
		ptr.prev = list.notSetupLast
		list.notSetupLast = ptr
	}
}

func (list *dataSrcList) removeContainerPtrNotSetup(ptr *dataSrcContainer) {
	prev := ptr.prev
	next := ptr.next

	if prev == nil && next == nil {
		list.notSetupHead = nil
		list.notSetupLast = nil
	} else if prev == nil {
		next.prev = nil
		list.notSetupHead = next
	} else if next == nil {
		prev.next = nil
		list.notSetupLast = prev
	} else {
		next.prev = prev
		prev.next = next
	}
}

func (list *dataSrcList) removeAndCloseLocalContainerPtrNotSetupByName(name string) {
	ptr := list.notSetupHead
	for ptr != nil {
		if ptr.local && ptr.name == name {
			list.removeContainerPtrNotSetup(ptr)
			ptr.ds.Close()
		}
		ptr = ptr.next
	}
}

func (list *dataSrcList) appendContainerPtrDidSetup(ptr *dataSrcContainer) {
	ptr.next = nil

	if list.didSetupLast == nil {
		list.didSetupHead = ptr
		list.didSetupLast = ptr
		ptr.prev = nil
	} else {
		list.didSetupLast.next = ptr
		ptr.prev = list.didSetupLast
		list.didSetupLast = ptr
	}
}

func (list *dataSrcList) removeContainerPtrDidSetup(ptr *dataSrcContainer) {
	prev := ptr.prev
	next := ptr.next

	if prev == nil && next == nil {
		list.didSetupHead = nil
		list.didSetupLast = nil
	} else if prev == nil {
		next.prev = nil
		list.didSetupHead = next
	} else if next == nil {
		prev.next = nil
		list.didSetupLast = prev
	} else {
		next.prev = prev
		prev.next = next
	}
}

func (list *dataSrcList) removeAndCloseLocalContainerPtrDidSetupByName(name string) {
	ptr := list.didSetupHead
	for ptr != nil {
		if ptr.local && ptr.name == name {
			list.removeContainerPtrDidSetup(ptr)
			ptr.ds.Close()
		}
		ptr = ptr.next
	}
}

func (list *dataSrcList) copyContainerPtrsDidSetupInto(m map[string]*dataSrcContainer) {
	ptr := list.didSetupHead
	for ptr != nil {
		m[ptr.name] = ptr
		ptr = ptr.next
	}
}

func (list *dataSrcList) addDataSrc(name string, ds DataSrc) {
	ptr := &dataSrcContainer{local: list.local, name: name, ds: ds}
	list.appendContainerPtrNotSetup(ptr)
}

func (list *dataSrcList) setupDataSrcs() map[string]errs.Err {
	errMap := make(map[string]errs.Err)

	if list.notSetupHead == nil {
		return errMap
	}

	ag := AsyncGroup{}

	ptr := list.notSetupHead
	for ptr != nil {
		ag.name = ptr.name
		if err := ptr.ds.Setup(&ag); err.IsNotOk() {
			errMap[ag.name] = err
			break
		}
		ptr = ptr.next
	}

	ag.joinAndPutErrorsInto(errMap)

	firstPtrNotSetupYet := ptr

	ptr = list.notSetupHead
	for ptr != nil && ptr != firstPtrNotSetupYet {
		next := ptr.next
		if _, ok := errMap[ptr.name]; !ok {
			list.removeContainerPtrNotSetup(ptr)
			list.appendContainerPtrDidSetup(ptr)
		}
		ptr = next
	}

	return errMap
}

func (list *dataSrcList) closeDataSrcs() {
	ptr := list.didSetupLast
	for ptr != nil {
		ptr.ds.Close()
		ptr = ptr.prev
	}
	list.notSetupHead = nil
	list.notSetupLast = nil
	list.didSetupHead = nil
	list.didSetupLast = nil
}
