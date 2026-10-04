package sabi

import (
	"container/list"
	_ "fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/sttk/errs"
)

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

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 1)
			assert.Len(t, manager.listReady, 0)

			assert.Equal(t, manager.listUnready[0].name, "foo")

			ds2 := NewAsyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 0)

			assert.Equal(t, manager.listUnready[0].name, "foo")
			assert.Equal(t, manager.listUnready[1].name, "bar")
		}()

		assert.Equal(t, logger.Len(), 2)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("remove", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			//defer manager.close()  // to see Close logs by remove

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
			manager.add("baz", &ds3)

			ds4 := NewAsyncDataSrc(4, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("close", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
			manager.add("baz", &ds3)

			ds4 := NewAsyncDataSrc(4, logger, Fail_None)
			manager.add("qux", &ds4)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 2)
		}()

		assert.Equal(t, logger.Len(), 8)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
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

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewAsyncDataSrc(2, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "AsyncDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setup but error", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_Setup)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_Setup)
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
			assert.Equal(t, errors[0].Err.Error(), "github.com/sttk/errs.Err {reason:XXX file:test-commons_test.go line:243}")
		}()

		assert.Equal(t, logger.Len(), 6)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setup but ds is nil", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			manager.add("foo", nil)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 1)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setup()
			assert.Len(t, errors, 0)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 0)
		}()

		assert.Equal(t, logger.Len(), 0)
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

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 2")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder and fail", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_Setup)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_Setup)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
			manager.add("baz", &ds3)

			ds4 := NewSyncDataSrc(4, logger, Fail_None)
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
			assert.Equal(t, errors[0].Err.Error(), "github.com/sttk/errs.Err {reason:XXX file:test-commons_test.go line:243}")
		}()

		assert.Equal(t, logger.Len(), 9)
		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1 failed")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 4")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder containing duplicated name and ok", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder containing duplicated name and ok (2)", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
			manager.add("baz", &ds3)

			ds4 := NewSyncDataSrc(4, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 4")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder but one of names is not used", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			ds3 := NewSyncDataSrc(3, logger, Fail_None)
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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 3")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 3")
		log = log.Next()
		assert.Nil(t, log)
	})

	t.Run("setupWithOrder but one of names is not used (2)", func(t *testing.T) {
		logger := list.New()

		func() {
			manager := newDataSrcManager(true)
			defer manager.close()

			ds1 := NewSyncDataSrc(1, logger, Fail_None)
			manager.add("foo", &ds1)

			ds2 := NewSyncDataSrc(2, logger, Fail_None)
			manager.add("bar", &ds2)

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 2)
			assert.Len(t, manager.listReady, 0)

			errors := manager.setupWithOrder([]string{"bar", "xxx", "foo"})

			assert.True(t, manager.local)
			assert.Len(t, manager.listUnready, 0)
			assert.Len(t, manager.listReady, 2)

			assert.Len(t, errors, 0)
		}()
	})

	t.Run("copyDsReadyToMap", func(t *testing.T) {
		logger := list.New()

		contMap := make(map[string]dataSrcContainer)

		manager := newDataSrcManager(true)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 0)

		manager = newDataSrcManager(true)
		ds1 := NewSyncDataSrc(1, logger, Fail_None)
		manager.add("foo", &ds1)
		errors := manager.setup()
		assert.Len(t, errors, 0)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 1)
		assert.True(t, contMap["foo"].local)
		assert.Equal(t, contMap["foo"].name, "foo")

		manager = newDataSrcManager(false)
		ds2 := NewAsyncDataSrc(2, logger, Fail_None)
		ds3 := NewSyncDataSrc(3, logger, Fail_None)
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

	t.Run("add -> copyDsReadyToMap -> remove -> copyDsReadyToMap", func(t *testing.T) {
		logger := list.New()

		manager := newDataSrcManager(true)

		ds1 := NewSyncDataSrc(1, logger, Fail_None)
		manager.add("foo", &ds1)

		ds2 := NewSyncDataSrc(2, logger, Fail_None)
		manager.add("bar", &ds2)

		errors := manager.setup()
		assert.Len(t, errors, 0)

		contMap := make(map[string]dataSrcContainer)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 2)
		assert.True(t, contMap["foo"].local)
		assert.Equal(t, contMap["foo"].name, "foo")
		assert.NotNil(t, contMap["foo"].ds)
		assert.True(t, contMap["bar"].local)
		assert.Equal(t, contMap["bar"].name, "bar")
		assert.NotNil(t, contMap["bar"].ds)

		manager.remove("foo")

		errors = manager.setup()
		assert.Len(t, errors, 0)

		contMap = make(map[string]dataSrcContainer)
		manager.copyDsReadyToMap(contMap)
		assert.Equal(t, len(contMap), 1)
		assert.True(t, contMap["bar"].local)
		assert.Equal(t, contMap["bar"].name, "bar")
		assert.NotNil(t, contMap["bar"].ds)
	})
}
