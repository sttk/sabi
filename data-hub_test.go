package sabi

import (
	"container/list"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

const (
	fail__not = iota
	fail__setup
	fail__create_data_conn
	fail__commit
	fail__pre_commit
)

type SyncDataSrc2 struct {
	id     int8
	fail   int8
	logger *list.List
}

func (ds *SyncDataSrc2) Setup(ag *AsyncGroup) errs.Err {
	if ds.fail == fail__setup {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc2 %d failed to setup", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc2 %d setupped", ds.id))
	return errs.Ok()
}
func (ds *SyncDataSrc2) Close() {
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc2 %d closed", ds.id))
}
func (ds *SyncDataSrc2) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == fail__create_data_conn {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc2 %d failed to create a DataConn", ds.id))
		return nil, errs.New("xxx")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc2 %d created DataConn", ds.id))
	conn := &SyncDataConn2{id: ds.id, logger: ds.logger, fail: ds.fail}
	return conn, errs.Ok()
}

type AsyncDataSrc2 struct {
	id     int8
	fail   int8
	logger *list.List
}

func (ds *AsyncDataSrc2) Setup(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(200 * time.Millisecond)
		if ds.fail == fail__setup {
			ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc2 %d failed to setup", ds.id))
			return errs.New("YYY")
		}
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc2 %d setupped", ds.id))
		return errs.Ok()
	})
	return errs.Ok()
}
func (ds *AsyncDataSrc2) Close() {
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc2 %d closed", ds.id))
}
func (ds *AsyncDataSrc2) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == fail__create_data_conn {
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc2 %d failed to create a DataConn", ds.id))
		return nil, errs.New("yyy")
	}
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc2 %d created DataConn", ds.id))
	conn := &AsyncDataConn2{id: ds.id, logger: ds.logger, fail: ds.fail}
	return conn, errs.Ok()
}

type SyncDataConn2 struct {
	id        int8
	committed bool
	fail      int8
	logger    *list.List
}

func (conn *SyncDataConn2) Commit(ag *AsyncGroup) errs.Err {
	if conn.fail == fail__commit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d failed to commit", conn.id))
		return errs.New("ZZZ")
	}
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d committed", conn.id))
	return errs.Ok()
}
func (conn *SyncDataConn2) PreCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == fail__pre_commit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d failed to pre commit", conn.id))
		return errs.New("zzz")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d pre committed", conn.id))
	return errs.Ok()
}
func (conn *SyncDataConn2) PostCommit(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d post committed", conn.id))
}
func (conn *SyncDataConn2) ShouldForceBack() bool {
	return conn.committed
}
func (conn *SyncDataConn2) Rollback(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d rollbacked", conn.id))
}
func (conn *SyncDataConn2) ForceBack(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d forced back", conn.id))
}
func (conn *SyncDataConn2) Close() {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn2 %d closed", conn.id))
}

type AsyncDataConn2 struct {
	id        int8
	committed bool
	fail      int8
	logger    *list.List
}

func (conn *AsyncDataConn2) Commit(ag *AsyncGroup) errs.Err {
	if conn.fail == fail__commit {
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d failed to commit", conn.id))
		return errs.New("VVV")
	}
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d committed", conn.id))
	return errs.Ok()
}
func (conn *AsyncDataConn2) PreCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == fail__pre_commit {
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d failed to pre commit", conn.id))
		return errs.New("vvv")
	}
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d pre committed", conn.id))
	return errs.Ok()
}
func (conn *AsyncDataConn2) PostCommit(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d post committed", conn.id))
}
func (conn *AsyncDataConn2) ShouldForceBack() bool {
	return conn.committed
}
func (conn *AsyncDataConn2) Rollback(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d rollbacked", conn.id))
}
func (conn *AsyncDataConn2) ForceBack(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d forced back", conn.id))
}
func (conn *AsyncDataConn2) Close() {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn2 %d closed", conn.id))
}

func ResetGlobalVariables() {
	globalDataSrcsFixed = false
	globalDataSrcList.closeDataSrcs()
}

func TestOfGlobalFunctions(t *testing.T) {
	t.Run("setup and shutdown", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__not, logger: logger})
		Uses("bar", &SyncDataSrc2{id: 2, fail: fail__not, logger: logger})

		ptr := globalDataSrcList.notSetupHead
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "foo")
		ptr = ptr.next
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "bar")
		ptr = ptr.next
		assert.Nil(t, ptr)

		assert.Nil(t, globalDataSrcList.didSetupHead)

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr := globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "bar")
			ptr = ptr.next
			assert.Nil(t, ptr)
		}()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("fail to setup", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__setup, logger: logger})
		Uses("bar", &SyncDataSrc2{id: 2, fail: fail__setup, logger: logger})

		ptr := globalDataSrcList.notSetupHead
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "foo")
		ptr = ptr.next
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "bar")
		ptr = ptr.next
		assert.Nil(t, ptr)

		assert.Nil(t, globalDataSrcList.didSetupHead)

		func() {
			err := Setup()
			switch r := err.Reason().(type) {
			case FailToSetupGlobalDataSrcs:
				err2, ok := r.Errors["foo"]
				assert.True(t, ok)
				assert.Equal(t, err2.Reason(), "YYY")

				err2, ok = r.Errors["bar"]
				assert.True(t, ok)
				assert.Equal(t, err2.Reason(), "XXX")
			default:
				assert.Fail(t, err.Error())
			}
		}()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 failed to setup")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 failed to setup")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("cannot add global data srcs after setup", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__not, logger: logger})

		ptr := globalDataSrcList.notSetupHead
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "foo")
		ptr = ptr.next
		assert.Nil(t, ptr)

		assert.Nil(t, globalDataSrcList.didSetupHead)

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr := globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.Nil(t, ptr)

			Uses("bar", &SyncDataSrc2{id: 2, fail: fail__setup, logger: logger})

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr = globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.Nil(t, ptr)
		}()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("do nothing if executing setup twice", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__not, logger: logger})

		ptr := globalDataSrcList.notSetupHead
		assert.NotNil(t, ptr)
		assert.Equal(t, ptr.name, "foo")
		ptr = ptr.next
		assert.Nil(t, ptr)

		assert.Nil(t, globalDataSrcList.didSetupHead)

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr := globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.Nil(t, ptr)

			err = Setup()
			assert.True(t, err.IsOk())

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr = globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.Nil(t, ptr)
		}()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestOfDatHubLocal(t *testing.T) {
	t.Run("new and close with no global data srcs", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		hub := NewDataHub()
		hubImpl := hub.(*dataHubImpl)

		assert.Nil(t, hubImpl.localDataSrcList.notSetupHead)
		assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
		assert.Equal(t, len(hubImpl.dataSrcMap), 0)
		assert.Equal(t, len(hubImpl.dataConnMap), 0)
		assert.False(t, hubImpl.fixed)
	})

	t.Run("new and close with global data srcs", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__not, logger: logger})
		Uses("bar", &SyncDataSrc2{id: 2, fail: fail__not, logger: logger})

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr := globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "foo")
			ptr = ptr.next
			assert.NotNil(t, ptr)
			assert.Equal(t, ptr.name, "bar")
			ptr = ptr.next
			assert.Nil(t, ptr)

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)

			assert.Nil(t, hubImpl.localDataSrcList.notSetupHead)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			assert.Nil(t, globalDataSrcList.notSetupHead)

			ptr = globalDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
		}()

		assert.Nil(t, globalDataSrcList.notSetupHead)
		assert.Nil(t, globalDataSrcList.didSetupHead)

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("uses and disuses", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, fail: fail__not, logger: logger})
		Uses("bar", &SyncDataSrc2{id: 2, fail: fail__not, logger: logger})

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)

			assert.Nil(t, hubImpl.localDataSrcList.notSetupHead)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Uses("baz", &SyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			ptr := hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Uses("qux", &AsyncDataSrc2{id: 4, fail: fail__not, logger: logger})
			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Disuses("foo") // do nothing because of global
			hub.Disuses("bar") // do nothing because of global
			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Disuses("baz")
			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Disuses("qux")
			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)
		}()

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("cannot add and remove data src between begin and end", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		func() {
			err := Setup()
			assert.True(t, err.IsOk())
			defer Shutdown()

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)

			ptr := hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 0)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Uses("baz", &SyncDataSrc2{id: 1, fail: fail__not, logger: logger})

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 0)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			assert.True(t, hub.begin().IsOk())

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 1)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.Uses("foo", &AsyncDataSrc2{id: 2, fail: fail__not, logger: logger})

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 1)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.Disuses("baz")

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 1)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.end()

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 1)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Uses("foo", &AsyncDataSrc2{id: 2, fail: fail__not, logger: logger})

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 1)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			hub.Disuses("baz")

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 0)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)
		}()

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("begin and end", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		err := Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)
			defer hub.Close()

			hub.Uses("baz", &SyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &AsyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			ptr := hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.localDataSrcList.didSetupHead)
			assert.Equal(t, len(hubImpl.dataSrcMap), 2)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			assert.True(t, hub.begin().IsOk())

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 4)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.end()

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Nil(t, hubImpl.dataConnList.head)
			assert.Equal(t, len(hubImpl.dataSrcMap), 4)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			return errs.Ok()
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("begin and end but fail sync", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		err := Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, logger: logger, fail: fail__not})
			hub.Uses("qux", &SyncDataSrc2{id: 4, logger: logger, fail: fail__setup})

			err := hub.begin()
			switch r := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Equal(t, len(r.Errors), 1)
				e, ok := r.Errors["qux"]
				assert.True(t, ok)
				assert.Equal(t, e.Reason(), "XXX")
			default:
				assert.Fail(t, err.Error())
			}

			ptr := hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Equal(t, len(hubImpl.dataSrcMap), 3)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.end()

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Equal(t, len(hubImpl.dataSrcMap), 3)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			return errs.Ok()
		})
		assert.True(t, err.IsOk())
	})

	t.Run("begin and end but fail async", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		err := Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			hubImpl := hub.(*dataHubImpl)
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, logger: logger, fail: fail__setup})
			hub.Uses("qux", &SyncDataSrc2{id: 4, logger: logger, fail: fail__not})

			err := hub.begin()
			switch r := err.Reason().(type) {
			case FailToSetupLocalDataSrcs:
				assert.Equal(t, len(r.Errors), 1)
				e, ok := r.Errors["baz"]
				assert.True(t, ok)
				assert.Equal(t, e.Reason(), "YYY")
			default:
				assert.Fail(t, err.Error())
			}

			ptr := hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Equal(t, len(hubImpl.dataSrcMap), 3)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.True(t, hubImpl.fixed)

			hub.end()

			ptr = hubImpl.localDataSrcList.notSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			ptr = hubImpl.localDataSrcList.didSetupHead
			assert.NotNil(t, ptr)
			ptr = ptr.next
			assert.Nil(t, ptr)
			assert.Equal(t, len(hubImpl.dataSrcMap), 3)
			assert.Equal(t, len(hubImpl.dataConnMap), 0)
			assert.False(t, hubImpl.fixed)

			return errs.Ok()
		})
		assert.True(t, err.IsOk())
	})

	t.Run("commit", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		err := Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()
			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			conn1, err1 = GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 = GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 = GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 = GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			assert.True(t, hub.commit().IsOk())

			return errs.Ok()
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("fail to cast new data conn", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("bar", &SyncDataSrc2{id: 2, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			_, err1 := GetDataConn[*SyncDataConn2](hub, "foo")
			switch r := err1.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "foo")
				assert.Equal(t, r.CastToType, "*sabi.SyncDataConn2")
			default:
				assert.Fail(t, err1.Error())
			}

			_, err2 := GetDataConn[*AsyncDataConn2](hub, "bar")
			switch r := err2.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "bar")
				assert.Equal(t, r.CastToType, "*sabi.AsyncDataConn2")
			default:
				assert.Fail(t, err2.Error())
			}

			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("fail to cast reused data conn", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("bar", &SyncDataSrc2{id: 2, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			_, err1 = GetDataConn[*SyncDataConn2](hub, "foo")
			switch r := err1.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "foo")
				assert.Equal(t, r.CastToType, "*sabi.SyncDataConn2")
			default:
				assert.Fail(t, err1.Error())
			}

			_, err2 = GetDataConn[*AsyncDataConn2](hub, "bar")
			switch r := err2.Reason().(type) {
			case FailToCastDataConn:
				assert.Equal(t, r.Name, "bar")
				assert.Equal(t, r.CastToType, "*sabi.AsyncDataConn2")
			default:
				assert.Fail(t, err2.Error())
			}

			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("fail to create data conn", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("bar", &SyncDataSrc2{id: 2, fail: fail__create_data_conn, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			_, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			switch r := err2.Reason().(type) {
			case FailToCreateDataConn:
				assert.Equal(t, r.Name, "bar")
				assert.Equal(t, r.DataConnType, "*sabi.SyncDataConn2")
			default:
				assert.Fail(t, err2.Error())
			}
			assert.Equal(t, err2.Cause().(errs.Err).Reason(), "xxx")

			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 failed to create a DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("fail to create data conn because of no data src", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("bar", &SyncDataSrc2{id: 2, fail: fail__create_data_conn, logger: logger})

			assert.True(t, hub.begin().IsOk())

			_, err1 := GetDataConn[*SyncDataConn2](hub, "baz")
			switch r := err1.Reason().(type) {
			case NoDataSrcToCreateDataConn:
				assert.Equal(t, r.Name, "baz")
				assert.Equal(t, r.DataConnType, "*sabi.SyncDataConn2")
			default:
				assert.Fail(t, err1.Error())
			}
			assert.Nil(t, err1.Cause())

			_, err2 := GetDataConn[*AsyncDataConn2](hub, "qux")
			switch r := err2.Reason().(type) {
			case NoDataSrcToCreateDataConn:
				assert.Equal(t, r.Name, "qux")
				assert.Equal(t, r.DataConnType, "*sabi.AsyncDataConn2")
			default:
				assert.Fail(t, err2.Error())
			}
			assert.Nil(t, err1.Cause())

			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("commit when no data conn", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())
			assert.True(t, hub.commit().IsOk())
			hub.end()

			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("commit but fail global sync", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__commit})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["bar"]
				assert.Equal(t, e.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 failed to commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("commit but fail global async", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__commit})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["foo"]
				assert.Equal(t, e.Reason(), "VVV")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 failed to commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("commit but fail local sync", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__commit, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["qux"]
				assert.Equal(t, e.Reason(), "ZZZ")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 failed to commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("commit but fail local async", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__commit, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["baz"]
				assert.Equal(t, e.Reason(), "VVV")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 failed to commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("pre commit but fail global sync", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__pre_commit})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["bar"]
				assert.Equal(t, e.Reason(), "zzz")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 failed to pre commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("pre commit but fail global async", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__pre_commit})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["foo"]
				assert.Equal(t, e.Reason(), "vvv")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 failed to pre commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("pre commit but fail local sync", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__pre_commit, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["qux"]
				assert.Equal(t, e.Reason(), "zzz")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 failed to pre commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("pre commit but fail local async", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__pre_commit, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			err := hub.commit()
			switch r := err.Reason().(type) {
			case FailToPreCommitDataConn:
				assert.Equal(t, len(r.Errors), 1)
				e := r.Errors["baz"]
				assert.Equal(t, e.Reason(), "vvv")
			default:
				assert.Fail(t, err.Error())
			}

			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 failed to pre commit")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("rollback", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			hub.rollback()
			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 rollbacked")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 rollbacked")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 rollbacked")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 rollbacked")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("force back", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			assert.True(t, hub.commit().IsOk())
			hub.rollback()
			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 forced back")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 forced back")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 forced back")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 forced back")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("post commit", func(t *testing.T) {
		ResetGlobalVariables()
		defer ResetGlobalVariables()

		logger := list.New()

		Uses("foo", &AsyncDataSrc2{id: 1, logger: logger, fail: fail__not})
		Uses("bar", &SyncDataSrc2{id: 2, logger: logger, fail: fail__not})

		Setup().IfOkThen(func() errs.Err {
			defer Shutdown()

			hub := NewDataHub()
			defer hub.Close()

			hub.Uses("baz", &AsyncDataSrc2{id: 3, fail: fail__not, logger: logger})
			hub.Uses("qux", &SyncDataSrc2{id: 4, fail: fail__not, logger: logger})

			assert.True(t, hub.begin().IsOk())

			conn1, err1 := GetDataConn[*AsyncDataConn2](hub, "foo")
			assert.True(t, err1.IsOk())
			assert.NotNil(t, conn1)

			conn2, err2 := GetDataConn[*SyncDataConn2](hub, "bar")
			assert.True(t, err2.IsOk())
			assert.NotNil(t, conn2)

			conn3, err3 := GetDataConn[*AsyncDataConn2](hub, "baz")
			assert.True(t, err3.IsOk())
			assert.NotNil(t, conn3)

			conn4, err4 := GetDataConn[*SyncDataConn2](hub, "qux")
			assert.True(t, err4.IsOk())
			assert.NotNil(t, conn4)

			hub.postCommit()
			hub.end()
			return errs.Ok()
		})

		elem := logger.Front()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 created DataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataConn2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataConn2 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 4 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 3 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "SyncDataSrc2 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "AsyncDataSrc2 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}
