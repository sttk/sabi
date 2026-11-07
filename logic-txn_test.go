package sabi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

func TestLogicTxn(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-0")
					return errs.Ok()
				}).
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-1")
					return errs.Ok()
				}).
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					conn, err = data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				EndTxn()

			assert.True(t, err.IsOk())
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"execute logic-0",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"execute logic-1",
			"execute logic-2",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#PostCommit 2",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run but run force runs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-0")
					return errs.New("logic-0 failed")
				}).
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-1")
					return errs.Ok()
				}).
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-3")
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicTxn#Run(logic-0)")
				assert.Equal(t, r.Errors[0].Err.Reason(), "logic-0 failed")
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"execute logic-0",
			"execute logic-3",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run or block then skip even run force", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-0")
					return errs.New("logic-0 failed")
				}).
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-1")
					return errs.Ok()
				}).
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-3")
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicTxn#RunOrBlock(logic-0)")
				assert.Equal(t, r.Errors[0].Err.Reason(), "logic-0 failed")
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"execute logic-0",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run force then skip run but run force runs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-0")
					return errs.New("logic-0 failed")
				}).
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-1")
					return errs.Ok()
				}).
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-3")
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicTxn#RunForce(logic-0)")
				assert.Equal(t, r.Errors[0].Err.Reason(), "logic-0 failed")
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"execute logic-0",
			"execute logic-2",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Commit))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-0")
					return errs.Ok()
				}).
				Run(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-1")
					return errs.Ok()
				}).
				RunForce(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("baz")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-3")
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "ZZZ")
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#New 3",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#Setup 3",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"execute logic-0",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"execute logic-1",
			"SyncDataSrc#CreateDataConn 3",
			"SyncDataConn#New 3",
			"execute logic-2",
			"execute logic-3",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#PreCommit 3",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2 failed",
			"SyncDataConn#Rollback 2",
			"SyncDataConn#Rollback 3",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 3",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("do Run but fail to cast DataHub", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewHogeDataHub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				Run(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicTxn#Run(logic-0)")
				switch r0 := ee0.Err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r0.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r0.ToType, "sabi.FailToCastData")
				default:
					panic(ee0.Err.Error())
				}
			default:
				panic(err.Reason())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("do RunForce but fail to cast DataHub", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewHogeDataHub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunForce(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicTxn#RunForce(logic-0)")
				switch r0 := ee0.Err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r0.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r0.ToType, "sabi.FailToCastData")
				default:
					panic(ee0.Err.Error())
				}
			default:
				panic(err.Reason())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("do RunOrBlock but fail to cast DataHub", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewHogeDataHub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				BeginTxn().
				RunOrBlock(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				EndTxn()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicTxn#RunOrBlock(logic-0)")
				switch r0 := ee0.Err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r0.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r0.ToType, "sabi.FailToCastData")
				default:
					panic(ee0.Err.Error())
				}
			default:
				panic(err.Reason())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: Run", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "Run logic panic")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic but panicked")
					panic("Run logic panic")
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic but panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: RunForce", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "Run logic panic")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				RunForce(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic but panicked")
					panic("Run logic panic")
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic but panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: RunOnBlock", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "Run logic panic")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic but panicked")
					panic("Run logic panic")
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic but panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: PreCommit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_PreCommitPanicked))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "preCommit panicked")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic")
					return errs.Ok()
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic",
			"SyncDataConn#PreCommit 1 panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: Commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_CommitPanicked))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "commit panicked")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic")
					return errs.Ok()
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1 panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: PostCommit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_PostCommitPanicked))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "postCommit panicked")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic")
					return errs.Ok()
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1 panicked",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: Rollback", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_RollbackPanicked))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_RollbackPanicked))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "rollback panicked")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic but error")
					return errs.New("run logic error")
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic but error",
			"SyncDataConn#Rollback 1 panicked",
			"SyncDataConn#Rollback 2 panicked",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Abend 2",
			"SyncDataConn#Abend 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("panic: Close", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_ClosePanicked))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_ClosePanicked))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.False(t, runner.dataAcc.fixed)

			defer func() {
				p := recover()
				assert.Equal(t, p, "close panicked")

				assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
				assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
				assert.True(t, runner.dataAcc.localDataSrcManager.local)
				assert.Len(t, runner.dataAcc.dataSrcMap, 0)
				assert.Len(t, runner.dataAcc.dataConnMap, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
				assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
				assert.True(t, runner.dataAcc.fixed)
			}()

			runner.
				BeginTxn().
				Run(func(data *DataAcc) errs.Err {
					conn1, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn1)
					conn2, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic")
					return errs.Ok()
				}).
				EndTxn()
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#PostCommit 2",
			"SyncDataConn#Close 2 panicked",
			"SyncDataConn#Close 1 panicked",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})
}
