package sabi

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type SyncDataSrc struct {
	id       int8
	willFail bool
	logger   *list.List
}

func NewSyncDataSrc(id int8, logger *list.List, willFail bool) *SyncDataSrc {
	return &SyncDataSrc{id: id, logger: logger, willFail: willFail}
}
func (ds *SyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	logger := ds.logger
	if ds.willFail {
		logger.PushBack(fmt.Sprintf("SyncDataSrc %d failed to setup", ds.id))
		return errs.New("XXX")
	}
	logger.PushBack(fmt.Sprintf("SyncDataSrc %d setupped", ds.id))
	return errs.Ok()
}
func (ds *SyncDataSrc) Close() {
	logger := ds.logger
	logger.PushBack(fmt.Sprintf("SyncDataSrc %d closed", ds.id))
}
func (ds *SyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	logger := ds.logger
	logger.PushBack(fmt.Sprintf("SyncDataSrc %d created DataConn", ds.id))
	conn := &SyncDataConn{}
	return conn, errs.Ok()
}

type AsyncDataSrc struct {
	id       int8
	willFail bool
	logger   *list.List
}

func NewAsyncDataSrc(id int8, logger *list.List, willFail bool) *AsyncDataSrc {
	return &AsyncDataSrc{id: id, logger: logger, willFail: willFail}
}
func (ds *AsyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	logger := ds.logger
	ag.Add(func() errs.Err {
		if ds.willFail {
			logger.PushBack(fmt.Sprintf("AsyncDataSrc %d failed to setup", ds.id))
			return errs.New("XXX")
		}
		logger.PushBack(fmt.Sprintf("AsyncDataSrc %d setupped", ds.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (ds *AsyncDataSrc) Close() {
	logger := ds.logger
	logger.PushBack(fmt.Sprintf("AsyncDataSrc %d closed", ds.id))
}
func (ds *AsyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	logger := ds.logger
	logger.PushBack(fmt.Sprintf("AsyncDataSrc %d created DataConn", ds.id))
	conn := &AsyncDataConn{}
	return conn, errs.Ok()
}

type SyncDataConn struct{}

func (conn *SyncDataConn) Commit(ag *AsyncGroup) errs.Err    { return errs.Ok() }
func (conn *SyncDataConn) PreCommit(ag *AsyncGroup) errs.Err { return errs.Ok() }
func (conn *SyncDataConn) PostCommit(ag *AsyncGroup)         {}
func (conn *SyncDataConn) ShouldForceBack() bool             { return false }
func (conn *SyncDataConn) Rollback(ag *AsyncGroup)           {}
func (conn *SyncDataConn) ForceBack(ag *AsyncGroup)          {}
func (conn *SyncDataConn) Close()                            {}

type AsyncDataConn struct{}

func (conn *AsyncDataConn) Commit(ag *AsyncGroup) errs.Err    { return errs.Ok() }
func (conn *AsyncDataConn) PreCommit(ag *AsyncGroup) errs.Err { return errs.Ok() }
func (conn *AsyncDataConn) PostCommit(ag *AsyncGroup)         {}
func (conn *AsyncDataConn) ShouldForceBack() bool             { return false }
func (conn *AsyncDataConn) Rollback(ag *AsyncGroup)           {}
func (conn *AsyncDataConn) ForceBack(ag *AsyncGroup)          {}
func (conn *AsyncDataConn) Close()                            {}

func TestOfDataSrc(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)
	})

	t.Run("appendContainerPtrNotSet", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrNotSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr1)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Nil(t, ptr1.next)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrNotSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr2)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Nil(t, ptr2.next)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeHeadContainerPtrNotSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}
		dsList.appendContainerPtrNotSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}
		dsList.appendContainerPtrNotSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}
		dsList.appendContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr2)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr2.prev)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeMiddleContainerPtrNotSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrNotSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrNotSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr1)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeLastContainerPtrNotSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrNotSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrNotSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr2)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Nil(t, ptr2.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeAllContainerPtrNotSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrNotSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrNotSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr1)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr2)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr2.prev)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Equal(t, dsList.notSetupHead, ptr3)
		assert.Equal(t, dsList.notSetupLast, ptr3)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		assert.Nil(t, ptr3.prev)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrNotSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		dsList.closeDataSrcs()
	})

	t.Run("appendContainerPtrDidSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrDidSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr1)

		assert.Nil(t, ptr1.prev)
		assert.Nil(t, ptr1.next)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrDidSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr2)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Nil(t, ptr2.next)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeHeadContainerPtrDidSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}
		dsList.appendContainerPtrDidSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}
		dsList.appendContainerPtrDidSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}
		dsList.appendContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr2)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr2.prev)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeMiddleContainerPtrDidSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrDidSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrDidSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr1)
		assert.Nil(t, ptr3.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeLastContainerPtrDidSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrDidSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrDidSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr2)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Nil(t, ptr2.next)

		dsList.closeDataSrcs()
	})

	t.Run("removeAllContainerPtrDidSetup", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrDidSetup(ptr1)

		ds2 := NewSyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrDidSetup(ptr2)

		ds3 := NewSyncDataSrc(3, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr1)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr1.prev)
		assert.Equal(t, ptr1.next, ptr2)
		assert.Equal(t, ptr2.prev, ptr1)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr1)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr2)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr2.prev)
		assert.Equal(t, ptr2.next, ptr3)
		assert.Equal(t, ptr3.prev, ptr2)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr2)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Equal(t, dsList.didSetupHead, ptr3)
		assert.Equal(t, dsList.didSetupLast, ptr3)

		assert.Nil(t, ptr3.prev)
		assert.Nil(t, ptr3.next)

		dsList.removeContainerPtrDidSetup(ptr3)

		assert.Equal(t, dsList.local, false)
		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.notSetupLast)
		assert.Nil(t, dsList.didSetupHead)
		assert.Nil(t, dsList.didSetupLast)

		dsList.closeDataSrcs()
	})

	t.Run("copyContainerPtrsDidSetupInto", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		m := make(map[string]*dataSrcContainer)
		dsList.copyContainerPtrsDidSetupInto(m)
		assert.Equal(t, len(m), 0)

		logger := list.New()

		ds1 := NewSyncDataSrc(1, logger, false)
		ptr1 := &dataSrcContainer{local: false, name: "foo", ds: ds1}

		dsList.appendContainerPtrDidSetup(ptr1)

		ds2 := NewAsyncDataSrc(2, logger, false)
		ptr2 := &dataSrcContainer{local: false, name: "bar", ds: ds2}

		dsList.appendContainerPtrDidSetup(ptr2)

		ds3 := NewAsyncDataSrc(2, logger, false)
		ptr3 := &dataSrcContainer{local: false, name: "baz", ds: ds3}

		dsList.appendContainerPtrDidSetup(ptr3)

		errMap := make(map[string]*dataSrcContainer)
		dsList.copyContainerPtrsDidSetupInto(errMap)

		assert.Equal(t, len(errMap), 3)
		assert.Equal(t, errMap["foo"], ptr1)
		assert.Equal(t, errMap["bar"], ptr2)
		assert.Equal(t, errMap["baz"], ptr3)

		dsList.closeDataSrcs()
	})

	t.Run("setupAndCreateDataConnAndClose", func(t *testing.T) {
		logger := list.New()

		dsList := dataSrcList{local: false}

		dsAsync := NewAsyncDataSrc(1, logger, false)
		dsList.addDataSrc("foo", dsAsync)

		dsSync := NewSyncDataSrc(2, logger, false)
		dsList.addDataSrc("bar", dsSync)

		errMap := dsList.setupDataSrcs()
		assert.Equal(t, len(errMap), 0)

		ptr := dsList.didSetupHead
		_, err := ptr.ds.CreateDataConn()
		assert.True(t, err.IsOk())

		ptr = ptr.next
		_, err = ptr.ds.CreateDataConn()
		assert.True(t, err.IsOk())

		dsList.closeDataSrcs()

		e := logger.Front()
		assert.Equal(t, e.Value, "SyncDataSrc 2 setupped")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 setupped")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 created DataConn")
		e = e.Next()
		assert.Equal(t, e.Value, "SyncDataSrc 2 created DataConn")
		e = e.Next()
		assert.Equal(t, e.Value, "SyncDataSrc 2 closed")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 closed")
	})

	t.Run("failToSetupSyncAndClose", func(t *testing.T) {
		logger := list.New()

		dsList := dataSrcList{local: true}

		dsAsync := NewAsyncDataSrc(1, logger, false)
		dsList.addDataSrc("foo", dsAsync)

		dsSync := NewSyncDataSrc(2, logger, true)
		dsList.addDataSrc("bar", dsSync)

		errMap := dsList.setupDataSrcs()
		assert.Equal(t, len(errMap), 1)

		err := errMap["bar"]
		assert.Equal(t, err.Reason(), "XXX")

		dsList.closeDataSrcs()

		e := logger.Front()
		assert.Equal(t, e.Value, "SyncDataSrc 2 failed to setup")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 setupped")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 closed")
		e = e.Next()
		assert.Nil(t, e)
	})

	t.Run("failToSetupAsyncAndClose", func(t *testing.T) {
		logger := list.New()

		dsList := dataSrcList{local: true}

		dsAsync := NewAsyncDataSrc(1, logger, true)
		dsList.addDataSrc("foo", dsAsync)

		dsSync := NewSyncDataSrc(2, logger, false)
		dsList.addDataSrc("bar", dsSync)

		errMap := dsList.setupDataSrcs()
		assert.Equal(t, len(errMap), 1)

		err := errMap["foo"]
		assert.Equal(t, err.Reason(), "XXX")

		dsList.closeDataSrcs()

		e := logger.Front()
		assert.Equal(t, e.Value, "SyncDataSrc 2 setupped")
		e = e.Next()
		assert.Equal(t, e.Value, "AsyncDataSrc 1 failed to setup")
		e = e.Next()
		assert.Equal(t, e.Value, "SyncDataSrc 2 closed")
		e = e.Next()
		assert.Nil(t, e)
	})

	t.Run("noDataSrc", func(t *testing.T) {
		dsList := dataSrcList{local: false}

		errMap := dsList.setupDataSrcs()
		assert.Equal(t, len(errMap), 0)

		dsList.closeDataSrcs()

		assert.Nil(t, dsList.notSetupHead)
		assert.Nil(t, dsList.didSetupHead)
	})
}
