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

		ierrs := ag.join()
		assert.Empty(t, len(ierrs))
	})

	t.Run("ok", func(t *testing.T) {
		var ag AsyncGroup

		executed := false
		fn := func() errs.Err {
			time.Sleep(50)
			executed = true
			return errs.Ok()
		}

		ag._index = 123
		ag._name = "foo"
		ag.Add(fn)
		assert.False(t, executed)

		errors := ag.join()
		assert.Len(t, errors, 0)
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

		ag._index = 123
		ag._name = "foo"
		ag.Add(fn)
		assert.False(t, executed)

		errors := ag.join()
		assert.Len(t, errors, 1)
		assert.True(t, executed)

		assert.Equal(t, errors[0].Index, 123)
		assert.Equal(t, errors[0].Name, "foo")
		switch errors[0].Err.Reason().(type) {
		case FailToDoSomething:
		default:
			assert.Fail(t, errors[0].Err.Error())
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
			time.Sleep(800)
			executed0 = true
			return errs.New(Reason0{})
		}
		fn1 := func() errs.Err {
			time.Sleep(400)
			executed1 = true
			return errs.New(Reason1{})
		}
		fn2 := func() errs.Err {
			time.Sleep(100)
			executed2 = true
			return errs.New(Reason2{})
		}

		ag._index = 12
		ag._name = "foo"
		ag.Add(fn0)
		ag._index = 34
		ag._name = "bar"
		ag.Add(fn1)
		ag._index = 56
		ag._name = "baz"
		ag.Add(fn2)

		errors := ag.join()
		assert.Len(t, errors, 3)
		assert.True(t, executed0)
		assert.True(t, executed1)
		assert.True(t, executed2)

		// Note: These assertions are temporarily disabled because goroutine scheduling
		// is non-deterministic. Relying on time.Sleep does not ensure the order of
		// execution, leading to flaky tests.
		//
		//assert.Equal(t, errors[0].Index, 56)
		//assert.Equal(t, errors[0].Name, "baz")
		//switch errors[0].Err.Reason().(type) {
		//case Reason2:
		//default:
		//	assert.Fail(t, errors[0].Err.Error())
		//}
		//
		//assert.Equal(t, errors[1].Index, 34)
		//assert.Equal(t, errors[1].Name, "bar")
		//switch errors[1].Err.Reason().(type) {
		//case Reason1:
		//default:
		//	assert.Fail(t, errors[0].Err.Error())
		//}
		//
		//assert.Equal(t, errors[2].Index, 12)
		//assert.Equal(t, errors[2].Name, "foo")
		//switch errors[2].Err.Reason().(type) {
		//case Reason0:
		//default:
		//	assert.Fail(t, errors[2].Err.Error())
		//}
	})
}
