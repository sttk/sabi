package sabi

import (
	"container/list"
	"context"
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

func TestDataHub(t *testing.T) {
	t.Run("NewDataHub", func(t *testing.T) {
		hub := NewDataHub()
		defer hub.Close()

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("NewDataHubWithCommitOrder", func(t *testing.T) {
		hub := NewDataHubWithCommitOrder("bar", "qux", "foo")
		defer hub.Close()

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Len(t, hubImpl.dataConnManager.list, 3)
		assert.Len(t, hubImpl.dataConnManager.indexMap, 3)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("Uses and ok", func(t *testing.T) {
		logger := list.New()

		hub := NewDataHub()
		defer hub.Close()

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
		hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		assert.True(t, hub.begin().IsOk())

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 2)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("Uses but already fixed", func(t *testing.T) {
		logger := list.New()

		hub := NewDataHub()
		defer hub.Close()

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		assert.True(t, hub.begin().IsOk())

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 1)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 1)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 1)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 1)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("Disuses and ok", func(t *testing.T) {
		logger := list.New()

		hub := NewDataHub()
		defer hub.Close()

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
		hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("foo")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("bar")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("Disuses and fix", func(t *testing.T) {
		logger := list.New()

		hub := NewDataHub()
		defer hub.Close()

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
		hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 2)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("foo")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 1)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("bar")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
		hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

		assert.True(t, hub.begin().IsOk())

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 2)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("foo")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 2)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("bar")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 2)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.end()

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 2)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("foo")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 1)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 1)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.Disuses("bar")

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Len(t, hubImpl.dataSrcMap, 0)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("begin if empty", func(t *testing.T) {
		hub := NewDataHub()
		defer hub.Close()

		assert.True(t, hub.begin().IsOk())

		hubImpl := hub.(*dataHubImpl)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.True(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)

		hub.end()

		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
		assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
		assert.True(t, hubImpl.localDataSrcManager.local)
		assert.Empty(t, hubImpl.dataSrcMap)
		assert.Empty(t, hubImpl.dataConnManager.list)
		assert.Empty(t, hubImpl.dataConnManager.indexMap)
		assert.Empty(t, hubImpl.dataConnMap)
		assert.False(t, hubImpl.fixed)
		assert.Nil(t, hubImpl.origCtx)
		assert.Nil(t, hubImpl.ctx)
		assert.Nil(t, hubImpl.cancel)
	})

	t.Run("begin and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			hubImpl := hub.(*dataHubImpl)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 2)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
			assert.True(t, hubImpl.localDataSrcManager.local)
			assert.Empty(t, hubImpl.dataSrcMap)
			assert.Empty(t, hubImpl.dataConnManager.list)
			assert.Empty(t, hubImpl.dataConnManager.indexMap)
			assert.Empty(t, hubImpl.dataConnMap)
			assert.False(t, hubImpl.fixed)
			assert.Nil(t, hubImpl.origCtx)
			assert.Nil(t, hubImpl.ctx)
			assert.Nil(t, hubImpl.cancel)

			assert.True(t, hub.begin().IsOk())

			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
			assert.True(t, hubImpl.localDataSrcManager.local)
			assert.Len(t, hubImpl.dataSrcMap, 2)
			assert.Empty(t, hubImpl.dataConnManager.list)
			assert.Empty(t, hubImpl.dataConnManager.indexMap)
			assert.Empty(t, hubImpl.dataConnMap)
			assert.True(t, hubImpl.fixed)
			assert.Nil(t, hubImpl.origCtx)
			assert.Nil(t, hubImpl.ctx)
			assert.Nil(t, hubImpl.cancel)

			hub.end()

			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 0)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 2)
			assert.True(t, hubImpl.localDataSrcManager.local)
			assert.Len(t, hubImpl.dataSrcMap, 2)
			assert.Empty(t, hubImpl.dataConnManager.list)
			assert.Empty(t, hubImpl.dataConnManager.indexMap)
			assert.Empty(t, hubImpl.dataConnMap)
			assert.False(t, hubImpl.fixed)
			assert.Nil(t, hubImpl.origCtx)
			assert.Nil(t, hubImpl.ctx)
			assert.Nil(t, hubImpl.cancel)
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

	t.Run("begin but failed", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_Setup, logger))
			hub.Uses("baz", NewMyDataSrc(3, Failure_None, logger))

			hubImpl := hub.(*dataHubImpl)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listUnready), 3)
			assert.Equal(t, countDs(hubImpl.localDataSrcManager.listReady), 0)
			assert.True(t, hubImpl.localDataSrcManager.local)
			assert.Empty(t, hubImpl.dataSrcMap)
			assert.Empty(t, hubImpl.dataConnManager.list)
			assert.Empty(t, hubImpl.dataConnManager.indexMap)
			assert.Empty(t, hubImpl.dataConnMap)
			assert.False(t, hubImpl.fixed)
			assert.Nil(t, hubImpl.origCtx)
			assert.Nil(t, hubImpl.ctx)
			assert.Nil(t, hubImpl.cancel)

			err := hub.begin()
			defer hub.end()

			switch rsn := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Name, "bar")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}
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

	t.Run("run and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Run(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				return errs.Ok()
			})
			assert.True(t, err.IsOk())
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("run but failed to run logic", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Run(hub, func(data any) errs.Err {
				logger.PushBack("execute logic but fail")
				return errs.New("logic error")
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case string:
				assert.Equal(t, rsn, "logic error")
			default:
				assert.Fail(t, err.Error())
			}
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic but fail")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("run but fail to cast to specified DataHub", func(t *testing.T) {
		type MyData interface {
			GetXxx() (string, errs.Err)
		}

		func() {
			hub := NewDataHub()
			defer hub.Close()

			err := Run(hub, func(data MyData) errs.Err {
				return errs.Ok()
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToCastDataHub:
				assert.Equal(t, rsn.FromType, "sabi.DataHub")
				assert.Equal(t, rsn.ToType, "sabi.MyData")
			default:
				assert.Fail(t, err.Error())
			}
		}()
	})

	t.Run("run but fail to setup", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))

			err := Run(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				return errs.Ok()
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1 failed")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn and no data access and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				return errs.Ok()
			})
			assert.True(t, err.IsOk())
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn and has data access and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			assert.True(t, err.IsOk())
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
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but failed to run logic", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.New("logic error")
			})
			switch rsn := err.Reason().(type) {
			case string:
				assert.Equal(t, rsn, "logic error")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but failed to pre-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_PreCommit, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_PreCommit, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "pre commit error")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but failed to commit", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_Commit, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_Commit, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "commit error")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but failed to post-commit", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_PostCommit, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_PostCommit, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			switch rsn := err.Reason().(type) {
			case FailToPostCommitDataConn:
				assert.Len(t, rsn.Errors, 2)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "post commit error")
				assert.Equal(t, rsn.Errors[1].Name, "bar")
				assert.Equal(t, rsn.Errors[1].Err.Reason(), "post commit error")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but failed to rollback", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_Rollback, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_Rollback, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.New("logic error")
			})
			switch rsn := err.Reason().(type) {
			case string:
				assert.Equal(t, rsn, "logic error")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#Rollback 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn with commit order", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHubWithCommitOrder("bar", "foo")
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "bar")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			assert.True(t, err.IsOk())
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
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("txn but fail to cast to specified DataHub", func(t *testing.T) {
		type MyData interface {
			GetXxx() (string, errs.Err)
		}

		func() {
			hub := NewDataHub()
			defer hub.Close()

			err := Txn(hub, func(data MyData) errs.Err {
				return errs.Ok()
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToCastDataHub:
				assert.Equal(t, rsn.FromType, "sabi.DataHub")
				assert.Equal(t, rsn.ToType, "sabi.MyData")
			default:
				assert.Fail(t, err.Error())
			}
		}()
	})

	t.Run("txn but fail to setup", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_Setup, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				return errs.Ok()
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Len(t, rsn.Errors, 1)
				assert.Equal(t, rsn.Errors[0].Name, "foo")
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "setup error")
			default:
				assert.Fail(t, err.Error())
			}
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1 failed")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn cached", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				return errs.Ok()
			})
			assert.True(t, err.IsOk())
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#CreateDataConn 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PreCommit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Commit 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#PostCommit 1")
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
			hub := NewDataHub()
			defer hub.Close()

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				return err
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case NoDataSrcToCreateDataConn:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "execute logic")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("get data conn and created data conn is nil", func(t *testing.T) {
		logger := list.New()

		func() {
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_CreatedDataConnIsNil, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				return err
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case CreatedDataConnIsNil:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}
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
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_CreateDataConn, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				return err
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToCreateDataConn:
				assert.Equal(t, rsn.Name, "foo")
				assert.Equal(t, rsn.DataConnType, "*sabi.MyDataConn")
			default:
				assert.Fail(t, err.Error())
			}
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
			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))
			hub.Uses("bar", NewMyDataSrc(2, Failure_None, logger))

			err := Txn(hub, func(data any) errs.Err {
				logger.PushBack("execute logic")
				_, err := GetDataConn[*MyDataConn](data, "foo")
				assert.True(t, err.IsOk())
				_, err = GetDataConn[*BadDataConn](data, "bar")
				assert.True(t, err.IsNotOk())
				switch rsn := err.Reason().(type) {
				case FailToCastDataConn:
					assert.Equal(t, rsn.Name, "bar")
					assert.Equal(t, rsn.ToDataConnType, "*sabi.BadDataConn")
				default:
					assert.Fail(t, err.Error())
				}
				return err
			})
			assert.True(t, err.IsNotOk())
			switch rsn := err.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, rsn.Name, "bar")
				assert.Equal(t, rsn.ToDataConnType, "*sabi.BadDataConn")
			default:
				assert.Fail(t, err.Error())
			}
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
		assert.Equal(t, log.Value, "MyDataConn#Rollback 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Rollback 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#OnTxnFailure 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataConn#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "MyDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("use context.Context", func(t *testing.T) {
		logger := list.New()

		hub := NewDataHub()
		defer hub.Close()

		hub.Uses("foo", NewMyDataSrc(1, Failure_None, logger))

		err := Run(hub, func(data DataAcc) errs.Err {
			logger.PushBack("execute logic")
			assert.Nil(t, data.Context())
			return errs.Ok()
		})
		assert.True(t, err.IsOk())

		err = Txn(hub, func(data DataAcc) errs.Err {
			logger.PushBack("execute logic")
			assert.Nil(t, data.Context())
			return errs.Ok()
		})
		assert.True(t, err.IsOk())

		ctx := context.Background()
		hub.(*dataHubImpl).SetContext(ctx)

		err = Run(hub, func(data DataAcc) errs.Err {
			logger.PushBack("execute logic")
			assert.NotNil(t, data.Context())
			return errs.Ok()
		})
		assert.True(t, err.IsOk())

		err = Txn(hub, func(data DataAcc) errs.Err {
			logger.PushBack("execute logic")
			assert.NotNil(t, data.Context())
			return errs.Ok()
		})
		assert.True(t, err.IsOk())
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
		Uses("bar", NewMyDataSrc(1, Failure_Setup, logger))

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
		assert.Equal(t, log.Value, "MyDataSrc#Setup 1 failed")
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
