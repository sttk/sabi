package sabi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

func TestOfLogicRunner(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("new if global data srcs are not fixed yet", func(t *testing.T) {
		ResetGlobals()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("uses", func(t *testing.T) {
		logger := NewLogger()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
		runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		assert.True(t, runner.dataAcc.begin().IsOk())

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 2)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("uses but already fixed", func(t *testing.T) {
		logger := NewLogger()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 1)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		assert.True(t, runner.dataAcc.begin().IsOk())

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 1)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 1)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 1)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 1)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("disuses and ok", func(t *testing.T) {
		logger := NewLogger()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
		runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("foo")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 1)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("bar")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("disuses and fix", func(t *testing.T) {
		logger := NewLogger()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
		runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("foo")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 1)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("bar")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
		runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

		assert.True(t, runner.dataAcc.begin().IsOk())

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 2)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("foo")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 2)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("bar")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 2)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.True(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.dataAcc.end()

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 2)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("bar")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 1)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 1)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		runner.Disuses("foo")

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)
	})

	t.Run("run with no data src", func(t *testing.T) {
		logger := NewLogger()

		hub := NewDataHub(func(da *DataAcc) any {
			return da
		})
		runner := NewLogicRunner(hub)

		assert.True(t, runner.
			Run(func(da *DataAcc) errs.Err {
				logger.PushBack("Run the logic")
				return errs.Ok()
			}).IsOk())

		assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
		assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
		assert.True(t, runner.dataAcc.localDataSrcManager.local)
		assert.Len(t, runner.dataAcc.dataSrcMap, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
		assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
		assert.Len(t, runner.dataAcc.dataConnMap, 0)
		assert.False(t, runner.dataAcc.fixed)
		assert.NotNil(t, runner.dataAcc.dataHub)

		assert.Equal(t, logger.Logs(), []string{
			"Run the logic",
		})
	})

	t.Run("run with some data srcs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			assert.True(t, runner.
				Run(func(da *DataAcc) errs.Err {
					conn1, err := da.GetDataConn[*SyncDataConn]("foo")
					if err.IsNotOk() {
						return err
					}
					assert.NotNil(t, conn1)
					conn2, err := da.GetDataConn[*SyncDataConn]("bar")
					if err.IsNotOk() {
						return err
					}
					assert.NotNil(t, conn2)
					logger.PushBack("Run the logic")
					return errs.Ok()
				}).IsOk())

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("run but failed to begin", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Setup))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 3)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.
				Run(func(da *DataAcc) errs.Err {
					logger.PushBack("Run the logic")
					return errs.Ok()
				})

			switch r := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "XXX")
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#New 3",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2 failed",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("run but failed to run logic", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Run(func(da *DataAcc) errs.Err {
					logger.PushBack("Run the logic but failed")
					return errs.New("logic error")
				})

			switch r := err.Reason().(type) {
			case string:
				assert.Equal(t, r, "logic error")
			}

		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"Run the logic but failed",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("run but failed to create data conn", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_CreateDataConn))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Run(func(da *DataAcc) errs.Err {
					_, err := da.GetDataConn[*SyncDataConn]("foo")
					if err.IsNotOk() {
						return err
					}
					logger.PushBack("Run the logic")
					return errs.Ok()
				})

			switch r := err.Reason().(type) {
			case FailToCreateDataConn:
				assert.Equal(t, r.Name, "foo")
				assert.Equal(t, r.DataConnType, "*sabi.SyncDataConn")
			default:
				panic(err.Error())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1 failed",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("run but data conn is null", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewNilConnDataSrc(1, logger))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.
				Run(func(da *DataAcc) errs.Err {
					_, err := da.GetDataConn[*SyncDataConn]("foo")
					if err.IsNotOk() {
						return err
					}
					logger.PushBack("Run the logic")
					return errs.Ok()
				})

			switch r := err.Reason().(type) {
			case CreatedDataConnIsNil:
				assert.Equal(t, r.Name, "foo")
				assert.Equal(t, r.DataConnType, "*sabi.SyncDataConn")
			default:
				panic(err.Error())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"NilConnDataSrc#New 1",
			"SyncDataSrc#New 2",
			"NilConnDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"NilConnDataSrc#CreateDataConn 1",
			"SyncDataSrc#Close 2",
			"NilConnDataSrc#Close 1",
		})
	})

	t.Run("start with no data src", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			run := runner.Start()
			assert.True(t, run.End().IsOk())
		}()

		assert.Len(t, logger.Logs(), 0)
	})

	t.Run("start with some data srcs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			run := runner.Start()
			assert.True(t, run.End().IsOk())
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

	t.Run("start but failed to begin", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Setup))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			if err := runner.Start().End(); err.IsNotOk() {
				switch r := err.Reason().(type) {
				case FailToSetupLocalDataSrcs:
					assert.Len(t, r.Errors, 1)
					assert.Equal(t, r.Errors[0].Index, 1)
					assert.Equal(t, r.Errors[0].Name, "bar")
					assert.Equal(t, r.Errors[0].Err.Reason(), "XXX")
				default:
					panic(err.Error())
				}
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#New 3",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2 failed",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn with no data srcs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			assert.True(t, runner.Txn(func(da *DataAcc) errs.Err {
				logger.PushBack("Run the logic")
				return errs.Ok()
			}).IsOk())

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
		}()

		assert.Equal(t, logger.Logs(), []string{"Run the logic"})
	})

	t.Run("txn with some data srcs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.True(t, runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic")
				return errs.Ok()
			}).IsOk())

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to begin", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Setup))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 3)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				conn3, err := da.GetDataConn[*SyncDataConn]("baz")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})

			switch r := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, r.Errors, 1)
				assert.Equal(t, r.Errors[0].Index, 1)
				assert.Equal(t, r.Errors[0].Name, "bar")
				assert.Equal(t, r.Errors[0].Err.Reason(), "XXX")
			default:
				panic(err.Error())
			}

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 3)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#New 3",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2 failed",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to run logic", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic but failed")
				return errs.New("logic error")
			})

			switch r := err.Reason().(type) {
			case string:
				assert.Equal(t, r, "logic error")
			default:
				panic(err.Error())
			}
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
			"Run the logic but failed",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to pre commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_PreCommit))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.Txn(func(data *DataAcc) errs.Err {
				conn1, err := data.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := data.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				conn3, err := data.GetDataConn[*SyncDataConn]("baz")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "zzz")
			default:
				panic(err.Reason())
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
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"SyncDataSrc#CreateDataConn 3",
			"SyncDataConn#New 3",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2 failed",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#Rollback 2",
			"SyncDataConn#Rollback 3",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:LogicFailure Err:github.com/sttk/errs.Err {reason:zzz file:test-commons_test.go line:86}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 3",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Commit))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.Txn(func(data *DataAcc) errs.Err {
				conn1, err := data.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := data.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				conn3, err := data.GetDataConn[*SyncDataConn]("baz")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "ZZZ")
			default:
				panic(err.Reason())
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
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"SyncDataSrc#CreateDataConn 3",
			"SyncDataConn#New 3",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#PreCommit 3",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2 failed",
			"SyncDataConn#Rollback 2",
			"SyncDataConn#Rollback 3",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:CommitFailure Err:github.com/sttk/errs.Err {reason:ZZZ file:test-commons_test.go line:76}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 3",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to post commit", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_PostCommit))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.Txn(func(data *DataAcc) errs.Err {
				conn1, err := data.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := data.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				conn3, err := data.GetDataConn[*SyncDataConn]("baz")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "!!!")
			default:
				panic(err.Reason())
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
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"SyncDataSrc#CreateDataConn 3",
			"SyncDataConn#New 3",
			"Run the logic",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#PreCommit 3",
			"SyncDataConn#Commit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#Commit 3",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#PostCommit 2 failed",
			"SyncDataConn#PostCommit 3",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:PostCommitFailure Err:github.com/sttk/errs.Err {reason:!!! file:test-commons_test.go line:98}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByCommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByNotRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 3",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("txn but failed to rollback", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Rollback))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.Txn(func(data *DataAcc) errs.Err {
				conn1, err := data.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := data.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				conn3, err := data.GetDataConn[*SyncDataConn]("baz")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic but failed")
				return errs.New("fail")
			})
			switch rsn := err.Reason().(type) {
			case string:
				assert.Equal(t, rsn, "fail")
			default:
				panic(err.Reason())
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
			"SyncDataSrc#CreateDataConn 2",
			"SyncDataConn#New 2",
			"SyncDataSrc#CreateDataConn 3",
			"SyncDataConn#New 3",
			"Run the logic but failed",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#Rollback 2 failed",
			"SyncDataConn#Rollback 3",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 3",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:RollbackFailure Err:github.com/sttk/errs.Err {reason:??? file:test-commons_test.go line:111}}} {DataConnName:baz DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 3",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 3",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("BeginTxn with no data src", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			txn := runner.BeginTxn()
			assert.True(t, txn.EndTxn().IsOk())
		}()

		assert.Len(t, logger.Logs(), 0)
	})

	t.Run("BeginTxn with some data srcs", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			txn := runner.BeginTxn()
			assert.True(t, txn.EndTxn().IsOk())
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

	t.Run("BeginTxn but failed to begin", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunner(NewDataHub(func(da *DataAcc) any {
				return da
			}))
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_Setup))
			runner.Uses("baz", NewSyncDataSrc(3, logger, Fail_None))

			err := runner.BeginTxn().EndTxn()
			switch rsn := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "XXX")
			default:
				panic(err.Error())
			}
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#New 3",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2 failed",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("new", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunnerWithCommitOrder(hub, "bar", "foo")
			defer runner.Close()

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
		}()

		assert.Len(t, logger.Logs(), 0)
	})

	t.Run("new with commit order if global data srcs are not fixed", func(t *testing.T) {
		logger := NewLogger()
		ResetGlobals()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunnerWithCommitOrder(hub, "bar", "foo")
			defer runner.Close()

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
		}()

		assert.Len(t, logger.Logs(), 0)
	})

	t.Run("Txn with commit order", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunnerWithCommitOrder(hub, "bar", "foo")
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			assert.True(t, runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic")
				return errs.Ok()
			}).IsOk())

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#PostCommit 2",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#Close 1",
			"SyncDataConn#Close 2",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("GetDataConn by creating but failed to cast", func(t *testing.T) {
		logger := NewLogger()

		func() {
			hub := NewDataHub(func(da *DataAcc) any {
				return da
			})
			runner := NewLogicRunner(hub)
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*AsyncDataConn]("bar")
				if err.IsNotOk() {
					return err
				}
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})

			switch r := err.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "bar")
				assert.Equal(t, r.FromDataConnType, "*sabi.SyncDataConn")
				assert.Equal(t, r.ToDataConnType, "*sabi.AsyncDataConn")
			default:
				panic(err.Error())
			}

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#Close 2",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("GetDataConn by using cache", func(t *testing.T) {
		logger := NewLogger()

		func() {
			runner := NewLogicRunnerWithCommitOrder(NewDataHub(func(da *DataAcc) any {
				return da
			}), "bar", "foo")
			defer runner.Close()

			runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
			runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 2)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 0)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)

				conn1, err = da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err = da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})
			assert.True(t, err.IsOk())

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 2)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#PreCommit 2",
			"SyncDataConn#PreCommit 1",
			"SyncDataConn#Commit 2",
			"SyncDataConn#Commit 1",
			"SyncDataConn#PostCommit 2",
			"SyncDataConn#PostCommit 1",
			"SyncDataConn#Close 1",
			"SyncDataConn#Close 2",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("GetDataConn by using cache but failed to cast", func(t *testing.T) {
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
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("bar")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn2)

				conn3, err := da.GetDataConn[*AsyncDataConn]("bar")
				if err.IsNotOk() {
					return err
				}
				assert.NotNil(t, conn3)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})

			switch r := err.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "bar")
				assert.Equal(t, r.FromDataConnType, "*sabi.SyncDataConn")
				assert.Equal(t, r.ToDataConnType, "*sabi.AsyncDataConn")
			default:
				panic(err.Error())
			}

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
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
			"SyncDataConn#Rollback 1",
			"SyncDataConn#Rollback 2",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#OnTxnFailure 2",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}} {DataConnName:bar DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 2",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("GetDataConn but no corresponding data src", func(t *testing.T) {
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
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)

			err := runner.Txn(func(da *DataAcc) errs.Err {
				conn1, err := da.GetDataConn[*SyncDataConn]("foo")
				assert.True(t, err.IsOk())
				assert.NotNil(t, conn1)
				conn2, err := da.GetDataConn[*SyncDataConn]("xxx")
				if err.IsNotOk() {
					return err
				}
				assert.NotNil(t, conn2)
				logger.PushBack("Run the logic")
				return errs.Ok()
			})

			switch r := err.Reason().(type) {
			case NoDataSrcToCreateDataConn:
				assert.Equal(t, r.Name, "xxx")
				assert.Equal(t, r.DataConnType, "*sabi.SyncDataConn")
			default:
				panic(err.Error())
			}

			assert.Len(t, runner.dataAcc.localDataSrcManager.listUnready, 0)
			assert.Len(t, runner.dataAcc.localDataSrcManager.listReady, 2)
			assert.True(t, runner.dataAcc.localDataSrcManager.local)
			assert.Len(t, runner.dataAcc.dataSrcMap, 2)
			assert.Len(t, runner.dataAcc.dataConnManager.list, 0)
			assert.Len(t, runner.dataAcc.dataConnManager.indexMap, 0)
			assert.Len(t, runner.dataAcc.dataConnMap, 0)
			assert.False(t, runner.dataAcc.fixed)
			assert.NotNil(t, runner.dataAcc.dataHub)
		}()

		assert.Equal(t, logger.Logs(), []string{
			"SyncDataSrc#New 1",
			"SyncDataSrc#New 2",
			"SyncDataSrc#Setup 1",
			"SyncDataSrc#Setup 2",
			"SyncDataSrc#CreateDataConn 1",
			"SyncDataConn#New 1",
			"SyncDataConn#Rollback 1",
			"SyncDataConn#OnTxnFailure 1",
			"TxnFailureReport=[{DataConnName:foo DataConnType:*sabi.SyncDataConn Cause:{State:NoneByUncommitted Err:github.com/sttk/errs.Err {}} Rollback:{State:NoneByRolledBack Err:github.com/sttk/errs.Err {}}}]",
			"SyncDataConn#Close 1",
			"SyncDataSrc#Close 2",
			"SyncDataSrc#Close 1",
		})
	})

	t.Run("data acc aggregation to DataHub", func(t *testing.T) {
		t.Run("Run", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))
				err := runner.Run(HogeLogic)
				assert.True(t, err.IsOk())
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
				"SyncDataConn#Close 2",
				"SyncDataConn#Close 1",
				"SyncDataSrc#Close 2",
				"SyncDataSrc#Close 1",
			})
		})

		t.Run("Txn", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))
				err := runner.Txn(HogeLogic)
				assert.True(t, err.IsOk())
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

		t.Run("Run but fail to cast DataHub", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))
				err := runner.Run(func(data FailToCastData) errs.Err {
					return errs.Ok()
				})

				switch r := err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r.ToType, "sabi.FailToCastData")
				default:
					panic(err.Error())
				}
			}()

			assert.Equal(t, logger.Logs(), []string{
				"SyncDataSrc#New 1",
				"SyncDataSrc#New 2",
			})
		})

		t.Run("Txn but fail to cast DataHub", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))
				err := runner.Txn(func(data FailToCastData) errs.Err {
					return errs.Ok()
				})

				switch r := err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r.ToType, "sabi.FailToCastData")
				default:
					panic(err.Error())
				}
			}()

			assert.Equal(t, logger.Logs(), []string{
				"SyncDataSrc#New 1",
				"SyncDataSrc#New 2",
			})
		})

		t.Run("nest/run", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

				err := runner.Txn(FugaLogic)
				assert.True(t, err.IsOk())
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

		t.Run("nest/start run end", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

				err := runner.Txn(FugaFugaLogic)
				assert.True(t, err.IsOk())
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

		t.Run("nest/fail to cast", func(t *testing.T) {
			logger := NewLogger()

			func() {
				runner := NewLogicRunner(NewHogeDataHub)
				defer runner.Close()

				runner.Uses("foo", NewSyncDataSrc(1, logger, Fail_None))
				runner.Uses("bar", NewSyncDataSrc(2, logger, Fail_None))

				err := runner.Txn(FugaFugaFugaLogic)
				switch r := err.Reason().(type) {
				case FailToCastDataHub:
					assert.Equal(t, r.FromType, "*struct { sabi.GettingDataAcc; sabi.SettingDataAcc; sabi.NestingDataAcc }")
					assert.Equal(t, r.ToType, "sabi.FailToCastData")
				default:
					panic(err.Error())
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
	})
}
