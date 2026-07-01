package sabi

import (
	"container/list"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type Fail int

const (
	Fail_Not = iota
	Fail_Commit
	Fail_PreCommit
	Fail_PostCommit
	Fail_Rollback
	Fail_PreCommitBecomeCommitted
)

type SyncDataConn struct {
	id        int8
	committed bool
	fail      Fail
	logger    *list.List
}

func NewSyncDataConn(id int8, logger *list.List, fail Fail) SyncDataConn {
	return SyncDataConn{
		id:        id,
		committed: false,
		fail:      fail,
		logger:    logger,
	}
}
func (conn *SyncDataConn) Commit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_Commit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Commit %d failed", conn.id))
		return errs.New("ZZZ")
	}
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Commit %d", conn.id))
	return errs.Ok()
}
func (conn *SyncDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_PreCommit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PreCommit %d failed", conn.id))
		return errs.New("zzz")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PreCommit %d", conn.id))
	if conn.fail == Fail_PreCommitBecomeCommitted {
		conn.committed = true
	}
	return errs.Ok()
}
func (conn *SyncDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_PostCommit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PostCommit %d failed", conn.id))
		return errs.New("!!!")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PostCommit %d", conn.id))
	return errs.Ok()
}
func (conn *SyncDataConn) IsCommitted() bool {
	return conn.committed
}
func (conn *SyncDataConn) Rollback(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_Rollback {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Rollback %d failed", conn.id))
		return errs.New("???")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Rollback %d", conn.id))
	return errs.Ok()
}
func (conn *SyncDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#OnTxnFailure %d", conn.id))
	conn.logger.PushBack(fmt.Sprintf("TxnFailureReport=%+v", reports))
}
func (conn *SyncDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Close %d", conn.id))
}

type AsyncDataConn struct {
	id        int8
	committed bool
	fail      Fail
	logger    *list.List
}

func NewAsyncDataConn(id int8, logger *list.List, fail Fail) AsyncDataConn {
	return AsyncDataConn{
		id:        id,
		committed: false,
		fail:      fail,
		logger:    logger,
	}
}
func (conn *AsyncDataConn) Commit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		if conn.fail == Fail_Commit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Commit %d failed", conn.id))
			return errs.New("YYY")
		}
		conn.committed = true
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Commit %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (conn *AsyncDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		if conn.fail == Fail_PreCommit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PreCommit %d failed", conn.id))
			return errs.New("yyy")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PreCommit %d", conn.id))
		if conn.fail == Fail_PreCommitBecomeCommitted {
			conn.committed = true
		}
		return errs.Ok()
	})
	return errs.Ok()
}
func (conn *AsyncDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		if conn.fail == Fail_PostCommit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PostCommit %d failed", conn.id))
			return errs.New("!!!")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PostCommit %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (conn *AsyncDataConn) IsCommitted() bool {
	return conn.committed
}
func (conn *AsyncDataConn) Rollback(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		if conn.fail == Fail_Rollback {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Rollback %d failed", conn.id))
			return errs.New("???")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Rollback %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (conn *AsyncDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#OnTxnFailure %d", conn.id))
		conn.logger.PushBack(fmt.Sprintf("TxnFailureReport=%+v", reports))
		return errs.Ok()
	})
}
func (conn *AsyncDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Close %d", conn.id))
}

func TestDataConn(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		manager := newDataConnManager()
		assert.Empty(t, manager.list)
		assert.Empty(t, manager.indexMap)
	})

	t.Run("new with commit order", func(t *testing.T) {
		manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
		assert.Len(t, manager.list, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Nil(t, manager.list[2].conn)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 0)
		assert.Equal(t, manager.indexMap["baz"], 1)
	})

	t.Run("new and add", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManager()
		assert.Empty(t, manager.list)
		assert.Empty(t, manager.indexMap)

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, &conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "bar", conn: &conn2})
		assert.Len(t, manager.list, 2)
		assert.Len(t, manager.indexMap, 2)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.list[0].conn, &conn1)
		assert.Equal(t, manager.list[1].conn, &conn2)
	})

	t.Run("new and add when overlapping name", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManager()
		assert.Empty(t, manager.list)
		assert.Empty(t, manager.indexMap)

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, &conn1)

		conn2 := NewAsyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn2})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, &conn1)
	})

	t.Run("new with commit order and add", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
		assert.Len(t, manager.list, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Nil(t, manager.list[2].conn)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 0)
		assert.Equal(t, manager.indexMap["baz"], 1)

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "bar", conn: &conn2})
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.list[0].conn, &conn2)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)

		conn3 := NewSyncDataConn(3, logger, Fail_Not)
		manager.add(dataConnContainer{name: "qux", conn: &conn3})
		assert.Len(t, manager.indexMap, 4)
		assert.Equal(t, manager.list[0].conn, &conn2)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)
		assert.Equal(t, manager.list[3].conn, &conn3)
	})

	t.Run("new with commit order and add when overlapping name", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
		assert.Len(t, manager.list, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Nil(t, manager.list[2].conn)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 0)
		assert.Equal(t, manager.indexMap["baz"], 1)

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn2})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)

		conn3 := NewSyncDataConn(3, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn3})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, &conn1)
	})

	t.Run("find by name but none", func(t *testing.T) {
		manager := newDataConnManager()
		_, ok := manager.findByName("foo")
		assert.False(t, ok)
		_, ok = manager.findByName("bar")
		assert.False(t, ok)
	})

	t.Run("find by name", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManager()

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})

		conn2 := NewAsyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "bar", conn: &conn2})

		assert.Len(t, manager.indexMap, 2)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, &conn1)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.list[1].conn, &conn2)

		cont, ok := manager.findByName("foo")
		assert.True(t, ok)
		assert.Equal(t, cont.conn, &conn1)

		cont, ok = manager.findByName("bar")
		assert.True(t, ok)
		assert.Equal(t, cont.conn, &conn2)

		_, ok = manager.findByName("qux")
		assert.False(t, ok)
	})

	t.Run("find by name of ordered DataConn", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManagerWithCommitOrder([]string{"baz", "qux", "foo"})

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})

		conn2 := NewSyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "bar", conn: &conn2})

		conn3 := NewSyncDataConn(3, logger, Fail_Not)
		manager.add(dataConnContainer{name: "baz", conn: &conn3})

		assert.Len(t, manager.indexMap, 4)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.list[2].conn, &conn1)
		assert.Equal(t, manager.indexMap["bar"], 3)
		assert.Equal(t, manager.list[3].conn, &conn2)
		assert.Equal(t, manager.indexMap["qux"], 1)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.indexMap["baz"], 0)
		assert.Equal(t, manager.list[0].conn, &conn3)

		cont, ok := manager.findByName("foo")
		assert.True(t, ok)
		assert.Equal(t, cont.conn, &conn1)

		cont, ok = manager.findByName("bar")
		assert.True(t, ok)
		assert.Equal(t, cont.conn, &conn2)

		cont, ok = manager.findByName("baz")
		assert.True(t, ok)
		assert.Equal(t, cont.conn, &conn3)

		_, ok = manager.findByName("qux")
		assert.False(t, ok)
	})

	t.Run("new failure reports", func(t *testing.T) {
		logger := list.New()

		manager := newDataConnManager()

		reports := manager.newFailureReports()
		assert.Equal(t, len(reports), 0)

		conn1 := NewSyncDataConn(1, logger, Fail_Not)
		manager.add(dataConnContainer{name: "foo", conn: &conn1})

		conn2 := NewAsyncDataConn(2, logger, Fail_Not)
		manager.add(dataConnContainer{name: "bar", conn: &conn2})

		reports = manager.newFailureReports()
		assert.Equal(t, len(reports), 2)

		report := reports[0]
		assert.Equal(t, report.DataConnName, "foo")
		assert.Equal(t, report.DataConnType, "*sabi.SyncDataConn")

		report = reports[1]
		assert.Equal(t, report.DataConnName, "bar")
		assert.Equal(t, report.DataConnType, "*sabi.AsyncDataConn")
	})

	t.Run("commit and rollback ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Not)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()
			assert.Equal(t, manager.commit(reports), errs.Ok())
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit with order and rollback ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Not)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			conn3 := NewSyncDataConn(3, logger, Fail_Not)
			manager.add(dataConnContainer{name: "qux", conn: &conn3})

			reports := manager.newFailureReports()
			assert.Equal(t, manager.commit(reports), errs.Ok())
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 18)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2") // because of async
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2") // because of async
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2") // because of async
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 3")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first sync pre-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "zzz")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:data-conn_test.go line:51}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:data-conn_test.go line:51}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first async pre-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "zzz")
				assert.Equal(t, r.Errors[1].Name, "foo")
				assert.Equal(t, r.Errors[1].Err.Reason(), "yyy")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 10)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:data-conn_test.go line:117}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:data-conn_test.go line:51}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:data-conn_test.go line:117}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:data-conn_test.go line:51}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail second pre-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "yyy")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 10)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:data-conn_test.go line:117}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:data-conn_test.go line:117}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first sync commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 11)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first async commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn2 := NewAsyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: &conn2})

			conn1 := NewSyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: &conn1})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
				assert.Equal(t, r.Errors[1].Name, "foo")
				assert.Equal(t, r.Errors[1].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail second commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 11)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first sync post-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
				assert.Equal(t, r.Errors[1].Name, "bar")
				assert.Equal(t, r.Errors[1].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:62}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:62}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail first async post-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn1})

			conn2 := NewSyncDataConn(1, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "foo", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
				assert.Equal(t, r.Errors[1].Name, "bar")
				assert.Equal(t, r.Errors[1].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:62}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:62}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback but fail second post-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:data-conn_test.go line:131}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("only rollback and first is sync", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Not)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("only rollback and first is async", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_Not)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("only rollback and second rollback failed", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_Not)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:73}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:73}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("only rollback and first rollback failed", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Not)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:73}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:73}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback and first rollback failed then second rollback failed", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 11)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:145}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:data-conn_test.go line:42}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:data-conn_test.go line:145}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback and second commit failed then first rollback failed", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 11)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback and pre commit become committed and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			assert.True(t, err.IsOk())

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 10)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PostCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("commit and rollback and pre commit become committed but failed", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "foo", conn: &conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: &conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Len(), 10)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Commit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:data-conn_test.go line:105}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataConn#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})
}
