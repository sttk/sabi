package sabi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

func TestAsyncGroup(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		var ag AsyncGroup

		m := make(map[string]errs.Err, 0)
		ag.joinAndPutErrorsInto(m)
		assert.Equal(t, len(m), 0)
	})

	t.Run("ok", func(t *testing.T) {
		var ag AsyncGroup

		executed := false
		fn := func() errs.Err {
			time.Sleep(50)
			executed = true
			return errs.Ok()
		}

		ag.name = "foo"
		ag.Add(fn)
		assert.False(t, executed)

		m := make(map[string]errs.Err, 0)
		ag.joinAndPutErrorsInto(m)
		assert.Equal(t, len(m), 0)
		assert.True(t, executed)
	})

	t.Run("error", func(t *testing.T) {
		var ag AsyncGroup

		type FailToDoSomething struct{}

		executed := false
		fn := func() errs.Err {
			time.Sleep(50)
			executed = true
			return errs.New(FailToDoSomething{})
		}

		ag.name = "foo"
		ag.Add(fn)
		assert.False(t, executed)

		m := make(map[string]errs.Err, 0)
		ag.joinAndPutErrorsInto(m)
		assert.Equal(t, len(m), 1)
		assert.True(t, executed)

		switch m["foo"].Reason().(type) {
		case FailToDoSomething:
		default:
			assert.Fail(t, m["foo"].Error())
		}
	})

	t.Run("multiple errors with an error map", func(t *testing.T) {
		var ag AsyncGroup

		type Reason0 struct{}
		type Reason1 struct{}
		type Reason2 struct{}

		executed0 := false
		executed1 := false
		executed2 := false

		fn0 := func() errs.Err {
			time.Sleep(200)
			executed0 = true
			return errs.New(Reason0{})
		}
		fn1 := func() errs.Err {
			time.Sleep(400)
			executed1 = true
			return errs.New(Reason1{})
		}
		fn2 := func() errs.Err {
			time.Sleep(800)
			executed2 = true
			return errs.New(Reason2{})
		}

		ag.name = "foo0"
		ag.Add(fn0)
		ag.name = "foo1"
		ag.Add(fn1)
		ag.name = "foo2"
		ag.Add(fn2)

		m := make(map[string]errs.Err, 0)
		ag.joinAndPutErrorsInto(m)
		assert.Equal(t, len(m), 3)
		assert.True(t, executed0)
		assert.True(t, executed1)
		assert.True(t, executed2)

		assert.Equal(t, m["foo0"].Error(),
			"github.com/sttk/errs.Err { reason = github.com/sttk/sabi.Reason0, file = async-group_test.go, line = 82 }")
		assert.Equal(t, m["foo1"].Error(),
			"github.com/sttk/errs.Err { reason = github.com/sttk/sabi.Reason1, file = async-group_test.go, line = 87 }")
		assert.Equal(t, m["foo2"].Error(),
			"github.com/sttk/errs.Err { reason = github.com/sttk/sabi.Reason2, file = async-group_test.go, line = 92 }")
	})

	t.Run("multiple errors without an error map", func(t *testing.T) {
		var ag AsyncGroup

		type Reason0 struct{}
		type Reason1 struct{}
		type Reason2 struct{}

		executed0 := false
		executed1 := false
		executed2 := false

		fn0 := func() errs.Err {
			time.Sleep(200)
			executed0 = true
			return errs.New(Reason0{})
		}
		fn1 := func() errs.Err {
			time.Sleep(400)
			executed1 = true
			return errs.New(Reason1{})
		}
		fn2 := func() errs.Err {
			time.Sleep(800)
			executed2 = true
			return errs.New(Reason2{})
		}

		ag.name = "foo0"
		ag.Add(fn0)
		ag.name = "foo1"
		ag.Add(fn1)
		ag.name = "foo2"
		ag.Add(fn2)

		ag.joinAndIgnoreErrors()
		assert.True(t, executed0)
		assert.True(t, executed1)
		assert.True(t, executed2)
	})
}
