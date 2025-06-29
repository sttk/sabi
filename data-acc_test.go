package sabi

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type FooDataSrc struct {
	id        int8
	text      string
	logger    *list.List
	will_fail bool
}

func (ds *FooDataSrc) Setup(ag *AsyncGroup) errs.Err {
	if ds.will_fail {
		ds.logger.PushBack(fmt.Sprintf("FooDataSrc %d failed to setup", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc %d setupped", ds.id))
	return errs.Ok()
}

func (ds *FooDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc %d closed", ds.id))
}

func (ds *FooDataSrc) CreateDataConn() (DataConn, errs.Err) {
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc %d created FooDataConn", ds.id))
	conn := &FooDataConn{id: ds.id, text: ds.text, logger: ds.logger}
	return conn, errs.Ok()
}

type FooDataConn struct {
	id        int8
	text      string
	committed bool
	logger    *list.List
}

func (conn *FooDataConn) GetText() string {
	return conn.text
}

func (conn *FooDataConn) Commit(ag *AsyncGroup) errs.Err {
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d committed", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d pre committed", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) PostCommit(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d post committed", conn.id))
}

func (conn *FooDataConn) ShouldForceBack() bool {
	return conn.committed
}

func (conn *FooDataConn) Rollback(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d rollbacked", conn.id))
}

func (conn *FooDataConn) ForceBack(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d forced back", conn.id))
}

func (conn *FooDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn %d closed", conn.id))
}

type BarDataSrc struct {
	id        int8
	text      string
	logger    *list.List
	will_fail bool
}

func (ds *BarDataSrc) Setup(ag *AsyncGroup) errs.Err {
	if ds.will_fail {
		ds.logger.PushBack(fmt.Sprintf("BarDataSrc %d failed to setup", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc %d setupped", ds.id))
	return errs.Ok()
}

func (ds *BarDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc.text = %s", ds.text))
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc %d closed", ds.id))
}

func (ds *BarDataSrc) CreateDataConn() (DataConn, errs.Err) {
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc %d created BarDataConn", ds.id))
	conn := &BarDataConn{id: ds.id, text: ds.text, logger: ds.logger, ds: ds}
	return conn, errs.Ok()
}

type BarDataConn struct {
	id        int8
	text      string
	ds        *BarDataSrc
	committed bool
	logger    *list.List
}

func (conn *BarDataConn) SetText(s string) errs.Err {
	conn.text = s
	return errs.Ok()
}

func (conn *BarDataConn) Commit(ag *AsyncGroup) errs.Err {
	conn.committed = true
	conn.ds.text = conn.text
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d committed", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d pre committed", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) PostCommit(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d post committed", conn.id))
}

func (conn *BarDataConn) ShouldForceBack() bool {
	return conn.committed
}

func (conn *BarDataConn) Rollback(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d rollbacked", conn.id))
}

func (conn *BarDataConn) ForceBack(ag *AsyncGroup) {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d forced back", conn.id))
}

func (conn *BarDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn.text = %s", conn.text))
	conn.logger.PushBack(fmt.Sprintf("BarDataConn %d closed", conn.id))
}

///

type SampleData interface {
	GetValue() (string, errs.Err)
	SetValue(v string) errs.Err
}

func sample_logic(data SampleData) errs.Err {
	v, err := data.GetValue()
	if err.IsNotOk() {
		return err
	}
	return data.SetValue(v)
}

func failing_logic(_data SampleData) errs.Err {
	return errs.New("ZZZ")
}

type FooDataAcc struct {
	DataAcc
}

func (data *FooDataAcc) GetValue() (string, errs.Err) {
	conn, err := GetDataConn[*FooDataConn](data, "foo")
	if err.IsNotOk() {
		return "", err
	}
	return conn.GetText(), errs.Ok()
}

type BarDataAcc struct {
	DataAcc
}

func (data *BarDataAcc) SetValue(text string) errs.Err {
	conn, err := GetDataConn[*BarDataConn](data, "bar")
	if err.IsNotOk() {
		return err
	}
	return conn.SetText(text)
}

///

func TestLogicArgument(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		Uses("foo", &FooDataSrc{id: 1, text: "hello", logger: logger, will_fail: false})
		Uses("bar", &BarDataSrc{id: 2, logger: logger})

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			return sample_logic(hub.(SampleData))
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = ")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubRunUsingGlobal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		Uses("foo", &FooDataSrc{id: 1, text: "hello", logger: logger, will_fail: false})
		Uses("bar", &BarDataSrc{id: 2, logger: logger})

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			return Run(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = ")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubRunUsingLocal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 1, text: "hello", logger: logger, will_fail: false})
			hub.Uses("bar", &BarDataSrc{id: 2, logger: logger})

			return Run(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = ")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("test not run logic if fail to setup local data src", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 1, text: "hello", logger: logger, will_fail: true})
			hub.Uses("bar", &BarDataSrc{id: 2, logger: logger})

			return Run(hub, sample_logic)
		})
		switch r := err.Reason().(type) {
		case FailToSetupLocalDataSrcs:
			e := r.Errors["foo"]
			assert.Equal(t, e.Reason(), "XXX")
		default:
			assert.Fail(t, err.Error())
		}

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 failed to setup")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubRunUsingGlobalAndLocal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		Uses("bar", &BarDataSrc{id: 1, logger: logger})

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 2, text: "Hello", logger: logger, will_fail: false})

			return Run(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "BarDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 1 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = ")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubTxnUsingGlobal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		Uses("foo", &FooDataSrc{id: 1, text: "Hello", logger: logger, will_fail: false})
		Uses("bar", &BarDataSrc{id: 2, logger: logger})

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			return Txn(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubTxnUsingLocal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 1, text: "Hello", logger: logger, will_fail: false})
			hub.Uses("bar", &BarDataSrc{id: 2, logger: logger})

			return Txn(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("test not run logic if fail to setup local data src", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 1, text: "Hello", logger: logger, will_fail: true})
			hub.Uses("bar", &BarDataSrc{id: 2, logger: logger})

			return Txn(hub, sample_logic)
		})
		switch r := err.Reason().(type) {
		case FailToSetupLocalDataSrcs:
			e := r.Errors["foo"]
			assert.Equal(t, e.Reason(), "XXX")
		default:
			assert.Fail(t, err.Error())
		}

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 failed to setup")
		elem = elem.Next()
		assert.Nil(t, elem)
	})

	t.Run("test fail to run logic in txn and rollback", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 1, text: "Hello", logger: logger, will_fail: false})
			hub.Uses("bar", &BarDataSrc{id: 2, logger: logger})

			return Txn(hub, failing_logic)
		})
		assert.Equal(t, err.Reason(), "ZZZ")

		elem := logger.Front()
		assert.Equal(t, elem.Value, "FooDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = ")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}

func TestDataHubTxnUsingGlobalAndLocal(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		resetGlobalVariables()
		defer resetGlobalVariables()

		logger := list.New()

		Uses("bar", &BarDataSrc{id: 1, logger: logger})

		err := Setup().IfOkThen(func() errs.Err {
			defer Close()

			hub := func() DataHub {
				hub := NewDataHub()
				data := struct {
					DataHub
					*FooDataAcc
					*BarDataAcc
				}{
					DataHub:    hub,
					FooDataAcc: &FooDataAcc{DataAcc: hub},
					BarDataAcc: &BarDataAcc{DataAcc: hub},
				}
				return data
			}()
			defer hub.Close()

			hub.Uses("foo", &FooDataSrc{id: 2, text: "Hello", logger: logger, will_fail: false})

			return Txn(hub, sample_logic)
		})
		assert.True(t, err.IsOk())

		elem := logger.Front()
		assert.Equal(t, elem.Value, "BarDataSrc 1 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 setupped")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 created FooDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 1 created BarDataConn")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 2 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 1 pre committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 2 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 1 committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 2 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 1 post committed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataConn 1 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataConn 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "FooDataSrc 2 closed")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc.text = Hello")
		elem = elem.Next()
		assert.Equal(t, elem.Value, "BarDataSrc 1 closed")
		elem = elem.Next()
		assert.Nil(t, elem)
	})
}
