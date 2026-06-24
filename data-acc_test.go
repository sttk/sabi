package sabi_test

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
	"github.com/sttk/sabi"
)

type FooDataConn struct {
	id        int8
	text      string
	committed bool
	logger    *list.List
}

func NewFooDataConn(id int8, s string, logger *list.List) *FooDataConn {
	logger.PushBack(fmt.Sprintf("NewFooDataConn %d", id))
	return &FooDataConn{id: id, text: s, logger: logger}
}

func (conn *FooDataConn) GetText() string {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#GetText %d", conn.id))
	return conn.text
}

func (conn *FooDataConn) Commit(ag *sabi.AsyncGroup) errs.Err {
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#Commit %d", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#PreCommit %d", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) PostCommit(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#PostCommit %d", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) IsCommitted() bool {
	return conn.committed
}

func (conn *FooDataConn) Rollback(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#Rollback %d", conn.id))
	return errs.Ok()
}

func (conn *FooDataConn) OnTxnFailure(ag *sabi.AsyncGroup, reports []sabi.TxnFailureReport) {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#OnTxn %d", conn.id))
}

func (conn *FooDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("FooDataConn#Close %d", conn.id))
}

type FooDataSrc struct {
	id            int8
	text          string
	fail_to_setup bool
	logger        *list.List
}

func NewFooDataSrc(id int8, s string, logger *list.List, fail bool) *FooDataSrc {
	logger.PushBack(fmt.Sprintf("NewFooDataSrc %d", id))
	return &FooDataSrc{id: id, text: s, logger: logger, fail_to_setup: fail}
}

func (ds *FooDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err {
	if ds.fail_to_setup {
		ds.logger.PushBack(fmt.Sprintf("FooDataSrc#Setup %d failed", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc#Setup %d", ds.id))
	return errs.Ok()
}

func (ds *FooDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc#Close %d", ds.id))
}

func (ds *FooDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) {
	ds.logger.PushBack(fmt.Sprintf("FooDataSrc#CreateDataConn %d", ds.id))
	return NewFooDataConn(ds.id, ds.text, ds.logger), errs.Ok()
}

type BarDataConn struct {
	id        int8
	text      string
	committed bool
	logger    *list.List
}

func NewBarDataConn(id int8, s string, logger *list.List) *BarDataConn {
	logger.PushBack(fmt.Sprintf("NewBarDataConn %d", id))
	return &BarDataConn{id: id, text: s, logger: logger}
}

func (conn *BarDataConn) SetText(text string) {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#SetText %d", conn.id))
	conn.text = text
}

func (conn *BarDataConn) Commit(ag *sabi.AsyncGroup) errs.Err {
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#Commit %d", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#PreCommit %d", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) PostCommit(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#PostCommit %d", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) IsCommitted() bool {
	return conn.committed
}

func (conn *BarDataConn) Rollback(ag *sabi.AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#Rollback %d", conn.id))
	return errs.Ok()
}

func (conn *BarDataConn) OnTxnFailure(ag *sabi.AsyncGroup, reports []sabi.TxnFailureReport) {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#OnTxn %d", conn.id))
}

func (conn *BarDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("BarDataConn#Close %d", conn.id))
}

type BarDataSrc struct {
	id            int8
	text          string
	fail_to_setup bool
	logger        *list.List
}

func NewBarDataSrc(id int8, logger *list.List, fail bool) *BarDataSrc {
	logger.PushBack(fmt.Sprintf("NewBarDataSrc %d", id))
	return &BarDataSrc{id: id, logger: logger, fail_to_setup: fail}
}

func (ds *BarDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err {
	if ds.fail_to_setup {
		ds.logger.PushBack(fmt.Sprintf("BarDataSrc#Setup %d failed", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc#Setup %d", ds.id))
	return errs.Ok()
}

func (ds *BarDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc#Close %d", ds.id))
}

func (ds *BarDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) {
	ds.logger.PushBack(fmt.Sprintf("BarDataSrc#CreateDataConn %d", ds.id))
	return NewBarDataConn(ds.id, ds.text, ds.logger), errs.Ok()
}

type SampleData interface {
	GetValue() (string, errs.Err)
	SetValue(v string) errs.Err
}

func sampleLogic(data SampleData) errs.Err {
	v, err := data.GetValue()
	if err.IsNotOk() {
		return err
	}
	if err = data.SetValue(v); err.IsNotOk() {
		return err
	}
	v, err = data.GetValue()
	if err.IsNotOk() {
		return err
	}
	if err = data.SetValue(v); err.IsNotOk() {
		return err
	}
	return errs.Ok()
}

type FooDataAcc struct {
	sabi.DataAcc
}

func (da *FooDataAcc) GetValue() (string, errs.Err) {
	dc, err := sabi.GetDataConn[*FooDataConn](da, "foo")
	if err.IsNotOk() {
		return "", err
	}
	return dc.GetText(), errs.Ok()
}

type BarDataAcc struct {
	sabi.DataAcc
}

func (da *BarDataAcc) SetValue(text string) errs.Err {
	dc, err := sabi.GetDataConn[*BarDataConn](da, "bar")
	if err.IsNotOk() {
		return err
	}
	dc.SetText(text)
	return errs.Ok()
}

type SampleDataHub struct {
	sabi.DataHub
	*FooDataAcc
	*BarDataAcc
}

func NewSampleDataHub() sabi.DataHub {
	hub := sabi.NewDataHub()
	return SampleDataHub{
		DataHub:    hub,
		FooDataAcc: &FooDataAcc{DataAcc: hub},
		BarDataAcc: &BarDataAcc{DataAcc: hub},
	}
}

func TestRun(t *testing.T) {
	sabi.ResetGlobals()
	defer sabi.ResetGlobals()

	logger := list.New()

	func() {
		sabi.Uses("foo", NewFooDataSrc(1, "hello", logger, false))

		err := sabi.Setup()
		defer sabi.Shutdown()
		assert.True(t, err.IsOk())

		func() {
			data := NewSampleDataHub()
			defer data.Close()

			data.Uses("bar", NewBarDataSrc(2, logger, false))

			err = sabi.Run(data, sampleLogic)
			assert.True(t, err.IsOk())
		}()
	}()

	log := logger.Front()
	assert.Equal(t, log.Value, "NewFooDataSrc 1")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataSrc#Setup 1")
	log = log.Next()
	assert.Equal(t, log.Value, "NewBarDataSrc 2")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataSrc#Setup 2")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataSrc#CreateDataConn 1")
	log = log.Next()
	assert.Equal(t, log.Value, "NewFooDataConn 1")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataConn#GetText 1")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataSrc#CreateDataConn 2")
	log = log.Next()
	assert.Equal(t, log.Value, "NewBarDataConn 2")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataConn#SetText 2")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataConn#GetText 1")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataConn#SetText 2")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataConn#Close 2")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataConn#Close 1")
	log = log.Next()
	assert.Equal(t, log.Value, "BarDataSrc#Close 2")
	log = log.Next()
	assert.Equal(t, log.Value, "FooDataSrc#Close 1")
	log = log.Next()
	assert.Nil(t, log)
}

func TestTxn(t *testing.T) {
}
