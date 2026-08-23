package sabi

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type Failure uint8

const (
	Failure_None Failure = iota
	Failure_PreCommit
	Failure_Commit
	Failure_PostCommit
	Failure_Rollback
	Failure_Setup
	Failure_CreateDataConn
	Failure_CreatedDataConnIsNil
	Failure_NoDataSrcToCreateDataConn
)

type MyDataConn struct {
	id        uint8
	failure   Failure
	committed bool
	logger    *list.List
}

func NewMyDataConn(id uint8, failure Failure, logger *list.List) *MyDataConn {
	return &MyDataConn{
		id:        id,
		failure:   failure,
		committed: false,
		logger:    logger,
	}
}

func (dc *MyDataConn) IsCommitted() bool {
	return dc.committed
}

func (dc *MyDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	if dc.failure == Failure_PreCommit {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#PreCommit %d failed", dc.id))
		return errs.New("pre commit error")
	} else {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#PreCommit %d", dc.id))
		return errs.Ok()
	}
}

func (dc *MyDataConn) Commit(ag *AsyncGroup) errs.Err {
	if dc.failure == Failure_Commit {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#Commit %d failed", dc.id))
		return errs.New("commit error")
	} else {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#Commit %d", dc.id))
		return errs.Ok()
	}
}

func (dc *MyDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	if dc.failure == Failure_PostCommit {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#PostCommit %d failed", dc.id))
		return errs.New("post commit error")
	} else {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#PostCommit %d", dc.id))
		return errs.Ok()
	}
}

func (dc *MyDataConn) Rollback(ag *AsyncGroup) errs.Err {
	if dc.failure == Failure_Rollback {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#Rollback %d failed", dc.id))
		return errs.New("rollback error")
	} else {
		dc.logger.PushBack(fmt.Sprintf("MyDataConn#Rollback %d", dc.id))
		return errs.Ok()
	}
}

func (dc *MyDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	dc.logger.PushBack(fmt.Sprintf("MyDataConn#OnTxnFailure %d", dc.id))
}

func (dc *MyDataConn) Close() {
	dc.logger.PushBack(fmt.Sprintf("MyDataConn#Close %d", dc.id))
}

type MyDataSrc struct {
	id      uint8
	failure Failure
	logger  *list.List
}

func NewMyDataSrc(id uint8, failure Failure, logger *list.List) *MyDataSrc {
	return &MyDataSrc{
		id:      id,
		failure: failure,
		logger:  logger,
	}
}

func (ds *MyDataSrc) Setup(ag *AsyncGroup) errs.Err {
	if ds.failure == Failure_Setup {
		ds.logger.PushBack(fmt.Sprintf("MyDataSrc#Setup %d failed", ds.id))
		return errs.New("setup error")
	} else {
		ds.logger.PushBack(fmt.Sprintf("MyDataSrc#Setup %d", ds.id))
		return errs.Ok()
	}
}

func (ds *MyDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("MyDataSrc#Close %d", ds.id))
}

func (ds *MyDataSrc) CreateDataConn() (DataConn, errs.Err) {
	if ds.failure == Failure_CreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("MyDataSrc#CreateDataConn %d failed", ds.id))
		return nil, errs.New("eeee")
	}
	if ds.failure == Failure_CreatedDataConnIsNil {
		ds.logger.PushBack(fmt.Sprintf("MyDataSrc#CreateDataConn %d is nil", ds.id))
		return nil, errs.Ok()
	}
	if ds.failure == Failure_NoDataSrcToCreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("MyDataSrc#CreateDataConn %d is no data src", ds.id))
		return nil, errs.New("eeee2")
	}
	ds.logger.PushBack(fmt.Sprintf("MyDataSrc#CreateDataConn %d", ds.id))
	return NewMyDataConn(ds.id, ds.failure, ds.logger), errs.Ok()
}

type BadDataConn struct{}

func (dc *BadDataConn) IsCommitted() bool                                       { return true }
func (dc *BadDataConn) PreCommit(ag *AsyncGroup) errs.Err                       { return errs.Ok() }
func (dc *BadDataConn) Commit(ag *AsyncGroup) errs.Err                          { return errs.Ok() }
func (dc *BadDataConn) PostCommit(ag *AsyncGroup) errs.Err                      { return errs.Ok() }
func (dc *BadDataConn) Rollback(ag *AsyncGroup) errs.Err                        { return errs.Ok() }
func (dc *BadDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {}
func (dc *BadDataConn) Close()                                                  {}

func countDs(list []dataSrcContainer) int {
	n := 0
	for _, cont := range list {
		if cont.ds != nil {
			n++
		}
	}
	return n
}

func TestDataAcc(t *testing.T) {
	t.Run("NewDataAcc", func(t *testing.T) {
		da := NewDataAcc()
		defer da.close()

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)
	})

	t.Run("NewDataAccWithCommitOrder", func(t *testing.T) {
		da := NewDataAccWithCommitOrder("bar", "qux", "foo")
		defer da.close()

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Len(t, da.dataConnManager.list, 3)
		assert.Len(t, da.dataConnManager.indexMap, 3)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)
	})

	t.Run("uses and ok", func(t *testing.T) {
		logger := list.New()

		da := NewDataAcc()
		defer da.close()

		da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
		da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		assert.True(t, da.begin().IsOk())

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 2)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)
	})

	t.Run("uses but already fixed", func(t *testing.T) {
		logger := list.New()

		da := NewDataAcc()
		defer da.close()

		da.uses("foo", NewMyDataSrc(1, Failure_None, logger))

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		assert.True(t, da.begin().IsOk())

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 1)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 1)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)

		da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 1)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 1)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)
	})

	t.Run("disuses and ok", func(t *testing.T) {
		logger := list.New()

		da := NewDataAcc()
		defer da.close()

		da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
		da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("foo")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("bar")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)
	})

	t.Run("Disuses and still fixed", func(t *testing.T) {
		logger := list.New()

		da := NewDataAcc()
		defer da.close()

		da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
		da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("foo")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("bar")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
		da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.True(t, da.begin().IsOk())

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 2)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)

		da.disuses("foo")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 2)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)

		da.disuses("bar")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 2)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)

		da.end()

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 2)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("foo")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 1)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 1)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)

		da.disuses("bar")

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Len(t, da.dataSrcMap, 0)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)
	})

	t.Run("begin if empty", func(t *testing.T) {
		da := NewDataAcc()
		defer da.close()

		assert.True(t, da.begin().IsOk())

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.True(t, da.fixed)

		da.end()

		assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
		assert.True(t, da.localDataSrcManager.local)
		assert.Empty(t, da.dataSrcMap)
		assert.Empty(t, da.dataConnManager.list)
		assert.Empty(t, da.dataConnManager.indexMap)
		assert.Empty(t, da.dataConnMap)
		assert.False(t, da.fixed)
	})

	t.Run("begin and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
			da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
			assert.True(t, da.localDataSrcManager.local)
			assert.Empty(t, da.dataSrcMap)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.False(t, da.fixed)

			assert.True(t, da.begin().IsOk())

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
			assert.True(t, da.localDataSrcManager.local)
			assert.Len(t, da.dataSrcMap, 2)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.True(t, da.fixed)

			da.end()

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 0)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 2)
			assert.True(t, da.localDataSrcManager.local)
			assert.Len(t, da.dataSrcMap, 2)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.False(t, da.fixed)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("begin and fail", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
			da.uses("bar", NewMyDataSrc(2, Failure_Setup, logger))

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
			assert.True(t, da.localDataSrcManager.local)
			assert.Empty(t, da.dataSrcMap)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.False(t, da.fixed)

			err := da.begin()
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
			assert.True(t, da.localDataSrcManager.local)
			assert.Empty(t, da.dataSrcMap)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.True(t, da.fixed)

			da.end()

			assert.Equal(t, countDs(da.localDataSrcManager.listUnready), 2)
			assert.Equal(t, countDs(da.localDataSrcManager.listReady), 0)
			assert.True(t, da.localDataSrcManager.local)
			assert.Empty(t, da.dataSrcMap)
			assert.Empty(t, da.dataConnManager.list)
			assert.Empty(t, da.dataConnManager.indexMap)
			assert.Empty(t, da.dataConnMap)
			assert.False(t, da.fixed)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn cached", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_None, logger))

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			dc1, err := da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsOk())

			dc2, err := da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsOk())
			assert.True(t, dc1 == dc2)

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn cached but unmatched type", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_None, logger))

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			_, err = da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsOk())

			_, err = da.GetDataConn[*BadDataConn]("foo")
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.FromDataConnType, "*sabi.MyDataConn")
				assert.Equal(t, rsn.ToDataConnType, "*sabi.BadDataConn")
			default:
				assert.Fail(t, err.Error())
			}

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn and no data src to create data conn", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			_, err = da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case NoDataSrcToCreateDataConn:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn and created data conn is nil", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_CreatedDataConnIsNil, logger))

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			_, err = da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case CreatedDataConnIsNil:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1 is nil")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn and failed to create data conn", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_CreateDataConn, logger))

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			_, err = da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToCreateDataConn:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn and failed to cast data conn", func(t *testing.T) {
		logger := list.New()

		func() {
			da := NewDataAcc()
			defer da.close()

			da.uses("foo", NewMyDataSrc(1, Failure_None, logger))
			da.uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := da.begin()
			assert.True(t, err.IsOk())

			logger.PushBack("execute logic")

			_, err = da.GetDataConn[*MyDataConn]("foo")
			assert.True(t, err.IsOk())

			_, err = da.GetDataConn[*BadDataConn]("bar")
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, rsn.Name, "bar")
				assert.Equal(t, rsn.FromDataConnType, "*sabi.MyDataConn")
				assert.Equal(t, rsn.ToDataConnType, "*sabi.BadDataConn")
			default:
				assert.Fail(t, err.Error())
			}

			da.end()
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})
}

func ResetGlobals() {
	globalDataSrcsFixed = false
	globalDataSrcManager.close()
}

func TestGlobals(t *testing.T) {
	t.Run("Uses and Setup, and ok", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_None, logger))

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 1)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		func() {
			err := Setup()
			defer Shutdown()
			assert.True(t, err.IsOk())

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 1)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("Uses and Setup, but fail", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 1)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		func() {
			err := Setup()
			defer Shutdown()
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToSetupGlobalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 0)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 0)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1 failed")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("Uses and Setup, but already fixed before", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		err := Setup()
		assert.True(t, err.IsOk())

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Shutdown()

		log := logger.Front()
		assert.Nil(t, log)
	})

	t.Run("Uses and SetupWithOrder, and ok", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_None, logger))
		Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 2)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		func() {
			err := SetupWithOrder("bar", "foo")
			defer Shutdown()
			assert.True(t, err.IsOk())

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 2)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("Uses and SetupWithOrder, but fail", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))
		Uses("bar", NewMyDataSrc(2, Failure_Setup, logger))

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 2)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		func() {
			err := SetupWithOrder("bar", "foo")
			defer Shutdown()
			assert.True(t, err.IsNotOk())

			switch rsn := err.Reason().(type) {
			case FailToSetupGlobalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Index, 0)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 0)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2 failed")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("Uses and SetupWithOrder, but already fixed before", func(t *testing.T) {
		ResetGlobals()
		defer ResetGlobals()

		logger := list.New()

		assert.False(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		err := SetupWithOrder("bar", "foo")
		assert.True(t, err.IsOk())

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Shutdown()

		log := logger.Front()
		assert.Nil(t, log)
	})
}
