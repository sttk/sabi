package sabi

import (
	"container/list"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type Fail2 int

const (
	Fail2_Not Fail2 = iota
	Fail2_Setup
	Fail2_CreateDataConn
)

type SyncDataSrc struct {
	id     int8
	logger *list.List
	fail   Fail2
}

func NewSyncDataSrc(id int8, logger *list.List, fail Fail2) SyncDataSrc {
	logger.PushBack(fmt.Sprintf("SyncDataSrc.New %d", id))
	return SyncDataSrc{id: id, logger: logger, fail: fail}
}
func (ds *SyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	if ds.fail == Fail2_Setup {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc.Setup %d failed", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc.Setup %d", ds.id))
	return errs.Ok()
}
func (ds *SyncDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc.Close %d", ds.id))
}
func (ds *SyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == Fail2_CreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc.CreateDataConn %d failed", ds.id))
		return nil, errs.New("eeee")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc.CreateDataConn %d", ds.id))
	return &SyncDataConn{}, errs.Ok()
}

type AsyncDataSrc struct {
	id     int8
	logger *list.List
	fail   Fail2
	wait   time.Duration
}

func NewAsyncDataSrc(id int8, logger *list.List, fail Fail2) AsyncDataSrc {
	logger.PushBack(fmt.Sprintf("AsyncDataSrc.New %d", id))
	return AsyncDataSrc{id: id, logger: logger, fail: fail}
}
func (ds *AsyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(ds.wait)
		if ds.fail == Fail2_Setup {
			ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc.Setup %d failed", ds.id))
			return errs.New("XXX")
		}
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc.Setup %d", ds.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (ds *AsyncDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc.Close %d", ds.id))
}
func (ds *AsyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == Fail2_CreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc.CreateDataConn %d failed", ds.id))
		return nil, errs.New("eeee")
	}
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc.CreateDataConn %d", ds.id))
	return &AsyncDataConn{}, errs.Ok()
}

func TestDataSrc(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		manager := newDataSrcManager(true)
		assert.True(t, manager.local)
		assert.Len(t, manager.listUnready, 0)
		assert.Len(t, manager.listReady, 0)

		manager = newDataSrcManager(false)
		assert.False(t, manager.local)
		assert.Len(t, manager.listUnready, 0)
		assert.Len(t, manager.listReady, 0)
	})

	t.Run("add", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 1)
			assert.Len(t, manager.listReady, 0)

			assert.Equal(t, manager.listUnready[0].name, "foo")

			ds2 := NewAsyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 0)

			assert.Equal(t, manager.listUnready[0].name, "foo")
			assert.Equal(t, manager.listUnready[1].name, "bar")
		}()

		assert.Equal(t, logger.Len(), 2)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("remove", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			//defer manager.close()  // to see Close logs by remove

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			ds4 := NewAsyncDataSrc(4, logger, Fail2_Not)
			manager.add("qux", &ds4)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 2)

			manager.remove("baz")
			manager.remove("foo")
			manager.remove("qux")
			manager.remove("bar")
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("close", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			ds4 := NewAsyncDataSrc(4, logger, Fail2_Not)
			manager.add("qux", &ds4)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 2)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setup no data src", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 0)
		}()

		assert.Equal(t, logger.Len(), 0)
		log := logger.Front()
		assert.Nil(t, log)
	})

	t.Run("setup and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 2)
		}()

		assert.Equal(t, logger.Len(), 6)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc.Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setup but error", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Setup)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Setup)
			manager.add("bar", &ds3)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 3)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setup()

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 3)
			assert.Len(t, manager.listReady, 0)

			assert.Len(t, errors, 1)
			assert.Equal(t, errors[0].Index, 1)
			assert.Equal(t, errors[0].Name, "bar")
			assert.Equal(t, errors[0].Err.Error(), "github.com/sttk/errs.Err {reason:XXX file:data-src_test.go line:34}")
		}()

		assert.Equal(t, logger.Len(), 6)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder no data src", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"bar", "foo"})
			assert.Len(t, errors, 0)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 0)
		}()

		assert.Equal(t, logger.Len(), 0)
		log := logger.Front()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 3)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"bar", "foo", "xxx"})
			assert.Empty(t, errors)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 3)

			assert.Len(t, errors, 0)
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder and fail", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Setup)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Setup)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			ds4 := NewSyncDataSrc(4, logger, Fail2_Not)
			manager.add("qux", &ds4)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 4)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"qux", "baz", "foo"})

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 4)
			assert.Len(t, manager.listReady, 0)

			assert.Len(t, errors, 1)
			assert.Equal(t, errors[0].Index, 2)
			assert.Equal(t, errors[0].Name, "foo")
			assert.Equal(t, errors[0].Err.Error(), "github.com/sttk/errs.Err {reason:XXX file:data-src_test.go line:34}")
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 4")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder containing duplicated name and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 3)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"baz", "baz", "foo"})

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 3)

			assert.Len(t, errors, 0)
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder containing duplicated name and ok 2", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			ds4 := NewSyncDataSrc(4, logger, Fail2_Not)
			manager.add("qux", &ds4)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 4)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"baz", "foo", "baz", "qux"})

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 4)

			assert.Len(t, errors, 0)
		}()

		assert.Equal(t, logger.Len(), 12)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder but one of names is not used", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail2_Not)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
			manager.add("baz", &ds3)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 3)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"baz", "foo", "xxx"})

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 3)

			assert.Len(t, errors, 0)
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc.New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc.Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("copyDsReadyToMap", func(t *testing.T) {
		logger := list.New()

		contMap := make(map[string]dataSrcContainer)

		manager := newDataSrcManager(true)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 0)

		manager = newDataSrcManager(true)
		ds1 := NewSyncDataSrc(1, logger, Fail2_Not)
		manager.add("foo", &ds1)
		errors := manager.setup()
		assert.Len(t, errors, 0)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 1)
		assert.True(t, contMap["foo"].local)
		assert.Equal(t, contMap["foo"].name, "foo")

		manager = newDataSrcManager(false)
		ds2 := NewAsyncDataSrc(2, logger, Fail2_Not)
		ds3 := NewSyncDataSrc(3, logger, Fail2_Not)
		manager.add("bar", &ds2)
		manager.add("baz", &ds3)
		errors = manager.setup()
		assert.Len(t, errors, 0)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 3)
		assert.True(t, contMap["foo"].local)
		assert.Equal(t, contMap["foo"].name, "foo")
		assert.False(t, contMap["bar"].local)
		assert.Equal(t, contMap["bar"].name, "bar")
		assert.False(t, contMap["baz"].local)
		assert.Equal(t, contMap["baz"].name, "baz")
	})
}
