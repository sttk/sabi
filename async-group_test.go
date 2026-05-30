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
		ag.Add(fn)
		assert.False(t, executed)

		ierrs := ag.join()
		assert.Len(t, ierrs, 0)
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
		ag.Add(fn)
		assert.False(t, executed)

		ierrs := ag.join()
		assert.Len(t, ierrs, 1)
		assert.True(t, executed)

		assert.Equal(t, ierrs[0].Index, 123)
		switch ierrs[0].Err.Reason().(type) {
		case FailToDoSomething:
		default:
			assert.Fail(t, ierrs[0].Err.Error())
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

		ag._index = 12
		ag.Add(fn0)
		ag._index = 34
		ag.Add(fn1)
		ag._index = 56
		ag.Add(fn2)

		ierrs := ag.join()
		assert.Len(t, ierrs, 3)
		assert.True(t, executed0)
		assert.True(t, executed1)
		assert.True(t, executed2)
	})
}
