package sabi

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

type SampleDataConn struct {
	id     int8
	logger *list.List
}

func (conn *SampleDataConn) Commit(ag *AsyncGroup) errs.Err    { return errs.Ok() }
func (conn *SampleDataConn) PreCommit(ag *AsyncGroup) errs.Err { return errs.Ok() }
func (conn *SampleDataConn) PostCommit(ag *AsyncGroup)         {}
func (conn *SampleDataConn) ShouldForceBack() bool             { return false }
func (conn *SampleDataConn) Rollback(ag *AsyncGroup)           {}
func (conn *SampleDataConn) ForceBack(ag *AsyncGroup)          {}
func (conn *SampleDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("SampleDataConn %d closed", conn.id))
}

func TestDataConn(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		dcList := dataConnList{}

		assert.Nil(t, dcList.head)
		assert.Nil(t, dcList.last)
	})

	t.Run("appendContainer", func(t *testing.T) {
		dcList := dataConnList{}
		logger := list.New()

		dc1 := &SampleDataConn{id: 1, logger: logger}
		cont1 := &dataConnContainer{name: "foo", conn: dc1}

		dcList.appendContainer(cont1)

		assert.Equal(t, dcList.head, cont1)
		assert.Equal(t, dcList.last, cont1)

		dc2 := &SampleDataConn{id: 2, logger: logger}
		cont2 := &dataConnContainer{name: "bar", conn: dc2}
		dcList.appendContainer(cont2)

		assert.Equal(t, dcList.head, cont1)
		assert.Equal(t, dcList.last, cont2)
		assert.Nil(t, cont1.prev)
		assert.Equal(t, cont1.next, cont2)
		assert.Equal(t, cont2.prev, cont1)
		assert.Nil(t, cont2.next)

		dc3 := &SampleDataConn{id: 3, logger: logger}
		cont3 := &dataConnContainer{name: "baz", conn: dc3}
		dcList.appendContainer(cont3)

		assert.Equal(t, dcList.head, cont1)
		assert.Equal(t, dcList.last, cont3)
		assert.Nil(t, cont1.prev)
		assert.Equal(t, cont1.next, cont2)
		assert.Equal(t, cont2.prev, cont1)
		assert.Equal(t, cont2.next, cont3)
		assert.Equal(t, cont3.prev, cont2)
		assert.Nil(t, cont3.next)

		dcList.closeDataConns()

		assert.Nil(t, dcList.head)
		assert.Nil(t, dcList.last)

		log := logger.Front()
		assert.Equal(t, log.Value, "SampleDataConn 3 closed")
		log = log.Next()
		assert.Equal(t, log.Value, "SampleDataConn 2 closed")
		log = log.Next()
		assert.Equal(t, log.Value, "SampleDataConn 1 closed")
		log = log.Next()
		assert.Nil(t, log)
	})
}
