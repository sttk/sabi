package sabi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

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
		logger := NewLogger()

		manager := newDataConnManager()
		assert.Empty(t, manager.list)
		assert.Empty(t, manager.indexMap)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_None)
		manager.add(dataConnContainer{name: "bar", conn: conn2})
		assert.Len(t, manager.list, 2)
		assert.Len(t, manager.indexMap, 2)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.list[0].conn, conn1)
		assert.Equal(t, manager.list[1].conn, conn2)
	})

	t.Run("new and add when overlapping name", func(t *testing.T) {
		logger := NewLogger()

		manager := newDataConnManager()
		assert.Empty(t, manager.list)
		assert.Empty(t, manager.indexMap)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, conn1)

		conn2 := NewAsyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn2})
		assert.Len(t, manager.list, 1)
		assert.Len(t, manager.indexMap, 1)
		assert.Equal(t, manager.indexMap["foo"], 0)
		assert.Equal(t, manager.list[0].conn, conn1)
	})

	t.Run("new with commit order and add", func(t *testing.T) {
		logger := NewLogger()

		manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
		assert.Len(t, manager.list, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Nil(t, manager.list[2].conn)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 0)
		assert.Equal(t, manager.indexMap["baz"], 1)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_None)
		manager.add(dataConnContainer{name: "bar", conn: conn2})
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.list[0].conn, conn2)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)

		conn3 := NewSyncDataConn(3, logger, Fail_None)
		manager.add(dataConnContainer{name: "qux", conn: conn3})
		assert.Len(t, manager.indexMap, 4)
		assert.Equal(t, manager.list[0].conn, conn2)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)
		assert.Equal(t, manager.list[3].conn, conn3)
	})

	t.Run("new with commit order and add when overlapping name", func(t *testing.T) {
		logger := NewLogger()

		manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
		assert.Len(t, manager.list, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Nil(t, manager.list[2].conn)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 0)
		assert.Equal(t, manager.indexMap["baz"], 1)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)

		conn2 := NewAsyncDataConn(2, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn2})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)

		conn3 := NewSyncDataConn(3, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn3})
		assert.Len(t, manager.indexMap, 3)
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)
	})

	t.Run("new failure reports", func(t *testing.T) {
		logger := NewLogger()

		manager := newDataConnManager()

		reports := manager.newFailureReports()
		assert.Equal(t, len(reports), 0)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})

		conn2 := NewAsyncDataConn(2, logger, Fail_None)
		manager.add(dataConnContainer{name: "bar", conn: conn2})

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
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_None)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()
			assert.Equal(t, manager.commit(reports), errs.Ok())
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"AsyncDataConn#PostCommit 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit with order and rollback ok", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManagerWithCommitOrder([]string{"bar", "baz", "foo"})
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_None)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			conn3 := NewSyncDataConn(3, logger, Fail_None)
			manager.add(dataConnContainer{name: "qux", conn: conn3})

			reports := manager.newFailureReports()
			assert.Equal(t, manager.commit(reports), errs.Ok())
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#New 3",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 3",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 3",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#PostCommit 3",
			"AsyncDataConn#PostCommit 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:qux DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 1",
			"AsyncDataConn#Close 2",
		})
	})

	t.Run("commit and rollback but fail first sync pre-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "zzz")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1 failed",
			"SyncDataConn#Rollback 1",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail first async pre-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "zzz")
				assert.Equal(t, r.Errors[1].Index, 0)
				assert.Equal(t, r.Errors[1].Name, "foo")
				assert.Equal(t, r.Errors[1].Err.Reason(), "yyy")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"AsyncDataConn#New 1",
			"SyncDataConn#New 2",
			"SyncDataConn#PreCommit 2 failed",
			"AsyncDataConn#PreCommit 1 failed",
			"SyncDataConn#Rollback 2",
			"AsyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:test-commons_test.go line:164}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:test-commons_test.go line:164}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 2",
			"AsyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail second pre-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "yyy")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2 failed",
			"SyncDataConn#Rollback 1",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:test-commons_test.go line:164}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:yyy file:test-commons_test.go line:164}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail first sync commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1 failed",
			"SyncDataConn#Rollback 1",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail first async commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn2 := NewAsyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: conn2})

			conn1 := NewSyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: conn1})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
				assert.Equal(t, r.Errors[1].Index, 0)
				assert.Equal(t, r.Errors[1].Name, "foo")
				assert.Equal(t, r.Errors[1].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"AsyncDataConn#New 1",
			"SyncDataConn#New 2",
			"SyncDataConn#PreCommit 2",
			"AsyncDataConn#PreCommit 1",
			"SyncDataConn#Commit 2 failed",
			"AsyncDataConn#Commit 1 failed",
			"SyncDataConn#Rollback 2",
			"AsyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 2",
			"AsyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail second commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2 failed",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail first sync post-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
				assert.Equal(t, r.Errors[1].Index, 1)
				assert.Equal(t, r.Errors[1].Name, "bar")
				assert.Equal(t, r.Errors[1].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1 failed",
			"AsyncDataConn#PostCommit 2 failed",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail first async post-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn1})

			conn2 := NewSyncDataConn(1, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "foo", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 2)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
				assert.Equal(t, r.Errors[1].Index, 0)
				assert.Equal(t, r.Errors[1].Name, "bar")
				assert.Equal(t, r.Errors[1].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"AsyncDataConn#New 2",
			"SyncDataConn#New 1",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1 failed",
			"AsyncDataConn#PostCommit 2 failed",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"AsyncDataConn#Close 2",
		})
	})

	t.Run("commit and rollback but fail second post-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"AsyncDataConn#PostCommit 2 failed",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback but fail second post-commit and contains no-commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PostCommit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			conn3 := NewNoCommitDataConn(3, logger, Fail_None)
			manager.add(dataConnContainer{name: "baz", conn: conn3})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "!!!")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"NoCommitDataConn#PreCommit 3",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"NoCommitDataConn#Commit 3",
			"AsyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"NoCommitDataConn#PostCommit 3",
			"AsyncDataConn#PostCommit 2 failed",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.NoCommitDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"NoCommitDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.NoCommitDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:180}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.NoCommitDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"NoCommitDataConn#Close 3",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("only rollback and first is sync", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_None)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#Rollback 1",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("only rollback and first is async", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_None)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"AsyncDataConn#New 1",
			"SyncDataConn#New 2",
			"SyncDataConn#Rollback 2",
			"AsyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 2",
			"AsyncDataConn#Close 1",
		})
	})

	t.Run("only rollback and second rollback failed", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewAsyncDataConn(1, logger, Fail_None)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewSyncDataConn(2, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"AsyncDataConn#New 1",
			"SyncDataConn#New 2",
			"SyncDataConn#Rollback 2 failed",
			"AsyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}}]",
			"AsyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}}]",
			"SyncDataConn#Close 2",
			"AsyncDataConn#Close 1",
		})
	})

	t.Run("only rollback and first rollback failed", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_None)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()
			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#Rollback 1 failed",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback and first commit failed then second rollback failed", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "foo")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1 failed",
			"SyncDataConn#Rollback 1",
			"AsyncDataConn#Rollback 2 failed",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:197}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:197}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback and second commit failed then first rollback failed", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_Rollback)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"AsyncDataConn#Commit 2 failed",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback and pre commit become committed and ok", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			assert.True(t, err.IsOk())

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"SyncDataConn#PostCommit 1",
			"AsyncDataConn#PostCommit 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("commit and rollback and pre commit become committed but failed", func(t *testing.T) {
		logger := NewLogger()

		func() {
			manager := newDataConnManager()
			defer manager.close()

			conn1 := NewSyncDataConn(1, logger, Fail_PreCommitBecomeCommitted)
			manager.add(dataConnContainer{name: "foo", conn: conn1})

			conn2 := NewAsyncDataConn(2, logger, Fail_Commit)
			manager.add(dataConnContainer{name: "bar", conn: conn2})

			reports := manager.newFailureReports()

			err := manager.commit(reports)
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			manager.rollback(reports)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataConn#New 1",
			"AsyncDataConn#New 2",
			"SyncDataConn#PreCommit 1",
			"AsyncDataConn#PreCommit 2",
			"AsyncDataConn#Commit 2 failed",
			"AsyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.AsyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:YYY file:test-commons_test.go line:150}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"AsyncDataConn#Close 2",
			"SyncDataConn#Close 1",
		})
	})

	t.Run("preserve commit order after close", func(t *testing.T) {
		logger := NewLogger()

		manager := newDataConnManagerWithCommitOrder([]string{"baz", "bar"})
		assert.Len(t, manager.list, 2)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Len(t, manager.indexMap, 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)

		conn1 := NewSyncDataConn(1, logger, Fail_None)
		manager.add(dataConnContainer{name: "foo", conn: conn1})
		assert.Len(t, manager.list, 3)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Equal(t, manager.list[2].name, "foo")
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Equal(t, manager.list[2].conn, conn1)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)

		conn2 := NewSyncDataConn(2, logger, Fail_None)
		manager.add(dataConnContainer{name: "bar", conn: conn2})
		assert.Len(t, manager.list, 3)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Equal(t, manager.list[2].name, "foo")
		assert.Nil(t, manager.list[0].conn)
		assert.Equal(t, manager.list[1].conn, conn2)
		assert.Equal(t, manager.list[2].conn, conn1)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)

		conn3 := NewSyncDataConn(3, logger, Fail_None)
		manager.add(dataConnContainer{name: "baz", conn: conn3})
		assert.Len(t, manager.list, 3)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Equal(t, manager.list[2].name, "foo")
		assert.Equal(t, manager.list[0].conn, conn3)
		assert.Equal(t, manager.list[1].conn, conn2)
		assert.Equal(t, manager.list[2].conn, conn1)
		assert.Len(t, manager.indexMap, 3)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)

		conn4 := NewSyncDataConn(4, logger, Fail_None)
		manager.add(dataConnContainer{name: "qux", conn: conn4})
		assert.Len(t, manager.list, 4)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Equal(t, manager.list[2].name, "foo")
		assert.Equal(t, manager.list[3].name, "qux")
		assert.Equal(t, manager.list[0].conn, conn3)
		assert.Equal(t, manager.list[1].conn, conn2)
		assert.Equal(t, manager.list[2].conn, conn1)
		assert.Equal(t, manager.list[3].conn, conn4)
		assert.Len(t, manager.indexMap, 4)
		assert.Equal(t, manager.indexMap["foo"], 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)
		assert.Equal(t, manager.indexMap["qux"], 3)

		manager.close()
		assert.Len(t, manager.list, 2)
		assert.Equal(t, manager.list[0].name, "baz")
		assert.Equal(t, manager.list[1].name, "bar")
		assert.Nil(t, manager.list[0].conn)
		assert.Nil(t, manager.list[1].conn)
		assert.Len(t, manager.indexMap, 2)
		assert.Equal(t, manager.indexMap["bar"], 1)
		assert.Equal(t, manager.indexMap["baz"], 0)
	})
}
