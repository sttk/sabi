package sabi

import (
	"container/list"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/sttk/errs"
)

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

		ds1 := NewSyncDataSrc(1, logger, Fail_None)
		Uses("foo", &ds1)

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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
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

		ds1 := NewSyncDataSrc(1, logger, Fail_Setup)
		Uses("foo", &ds1)

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
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "XXX")
			default:
				assert.Fail(t, err.Error())
			}

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 0)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1 failed")
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

		ds1 := NewSyncDataSrc(1, logger, Fail_Setup)
		Uses("foo", &ds1)

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Shutdown()

		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
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

		ds1 := NewSyncDataSrc(1, logger, Fail_None)
		Uses("foo", &ds1)
		ds2 := NewSyncDataSrc(2, logger, Fail_None)
		Uses("bar", &ds2)

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
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Close 2")
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

		ds1 := NewSyncDataSrc(1, logger, Fail_Setup)
		Uses("foo", &ds1)
		ds2 := NewSyncDataSrc(2, logger, Fail_Setup)
		Uses("bar", &ds2)

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
				assert.Equal(t, rsn.Errors[0].Err.Reason(), "XXX")
			default:
				assert.Fail(t, err.Error())
			}

			assert.True(t, globalDataSrcsFixed)
			assert.False(t, globalDataSrcManager.local)
			assert.Len(t, globalDataSrcManager.listUnready, 0)
			assert.Len(t, globalDataSrcManager.listReady, 0)
		}()

		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#New 2")
		log = log.Next()
		assert.Equal(t, log.Value, "SyncDataSrc#Setup 2 failed")
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

		ds1 := NewSyncDataSrc(1, logger, Fail_Setup)
		Uses("foo", &ds1)

		assert.True(t, globalDataSrcsFixed)
		assert.False(t, globalDataSrcManager.local)
		assert.Len(t, globalDataSrcManager.listUnready, 0)
		assert.Len(t, globalDataSrcManager.listReady, 0)

		Shutdown()

		log := logger.Front()
		assert.Equal(t, log.Value, "SyncDataSrc#New 1")
		log = log.Next()
		assert.Nil(t, log)
	})
}
