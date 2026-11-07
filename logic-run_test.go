package sabi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

func TestLogicRun(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Start().
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
				End()

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
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run but run force runs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Start().
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
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicRun#Run(logic-0)")
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
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run but run or block then skip even run force", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Start().
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
					conn, err := data.GetDataConn[*SyncDataConn]("bar")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-2")
					return errs.Ok()
				}).
				RunOrBlock(func(data *DataAcc) errs.Err {
					conn, err := data.GetDataConn[*SyncDataConn]("foo")
					assert.True(t, err.IsOk())
					assert.NotNil(t, conn)
					logger.PushBack("execute logic-3")
					return errs.Ok()
				}).
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicRun#RunOrBlock(logic-0)")
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
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("fail to run force then skip run but run force runs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Start().
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
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 0)
				assert.Equal(t, r.Errors[0].Name, "LogicRun#RunForce(logic-0)")
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
			"SyncDataConn#Close 1",
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
				Start().
				Run(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicRun#Run(logic-0)")
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
				Start().
				RunForce(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicRun#RunForce(logic-0)")
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
				Start().
				RunOrBlock(func(data FailToCastData) errs.Err {
					return errs.Ok()
				}).
				End()

			switch r := err.Reason().(type) {
			case FailToRunLogic:
				assert.Len(t, r.Errors, 1)
				ee0 := r.Errors[0]
				assert.Equal(t, ee0.Index, 0)
				assert.Equal(t, ee0.Name, "LogicRun#RunOrBlock(logic-0)")
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
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
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
				Start().
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
				End()
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
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
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
				Start().
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
				End()
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

	t.Run("panic: RunOrBlock", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
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
				Start().
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
				End()
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

	t.Run("panic: Close", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(acc *DataAcc) any {
				return acc
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
				Start().
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
				End()
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
			"SyncDataConn#Close 2 panicked",
			"SyncDataConn#Close 1 panicked",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})
}
