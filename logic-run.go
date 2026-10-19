// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"fmt"

	"github.com/sttk/errs"
)

type logicErrAt int

const (
	logicErrAtBegin = iota
	logicErrAtRun
	logicErrAtBlock
)

// LogicRun coordinates the execution of multiple non-transactional business logic functions
// in a sequential method chain.
type LogicRun struct {
	dataAcc *DataAcc
	errAt   logicErrAt
	errors  []ErrEntry
	index   int
	nested  bool
}

func newLogicRun(da *DataAcc, nested bool) LogicRun {
	if !nested {
		if err := da.begin(); err.IsNotOk() {
			return LogicRun{
				dataAcc: da,
				errAt:   logicErrAtBegin,
				errors:  []ErrEntry{{Err: err}},
				index:   0,
				nested:  nested,
			}
		}
	}

	return LogicRun{
		dataAcc: da,
		errAt:   logicErrAtRun,
		errors:  []ErrEntry{},
		index:   0,
		nested:  nested,
	}
}

// Run executes the given business logic function if no errors have occurred so far in the
// chain. If an error has already occurred in a preceding step, this logic is skipped.
// It returns the LogicRun instance for method chaining.
func (run LogicRun) Run[D any](logic func(D) errs.Err) LogicRun {
	index := run.index
	run.index = index + 1

	switch run.errAt {
	case logicErrAtRun:
		if len(run.errors) == 0 {
			data, ok := run.dataAcc.dataHub.(D)
			if !ok {
				fromType := typeNameOf(run.dataAcc.dataHub)
				toType := typeNameOfTypeParam[D]()
				err := errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
				run.errors = append(run.errors, ErrEntry{
					Index: index,
					Name:  fmt.Sprintf("LogicRun#Run(logic-%d)", index),
					Err:   err,
				})
			} else {
				if err := logic(data); err.IsNotOk() {
					run.errors = append(run.errors, ErrEntry{
						Index: index,
						Name:  fmt.Sprintf("LogicRun#Run(logic-%d)", index),
						Err:   err,
					})
				}
			}
		}
	}
	return run
}

// RunForce executes the given business logic function regardless of whether errors occurred
// in preceding steps, unless execution was blocked by RunOrBlock.
// It returns the LogicRun instance for method chaining.
func (run LogicRun) RunForce[D any](logic func(D) errs.Err) LogicRun {
	index := run.index
	run.index = index + 1

	switch run.errAt {
	case logicErrAtRun:
		data, ok := run.dataAcc.dataHub.(D)
		if !ok {
			fromType := typeNameOf(run.dataAcc.dataHub)
			toType := typeNameOfTypeParam[D]()
			err := errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
			run.errors = append(run.errors, ErrEntry{
				Index: index,
				Name:  fmt.Sprintf("LogicRun#RunForce(logic-%d)", index),
				Err:   err,
			})
		} else {
			if err := logic(data); err.IsNotOk() {
				run.errors = append(run.errors, ErrEntry{
					Index: index,
					Name:  fmt.Sprintf("LogicRun#RunForce(logic-%d)", index),
					Err:   err,
				})
			}
		}
	}
	return run
}

// RunOrBlock executes the given business logic function if no errors have occurred so far in
// the chain. If this logic function fails (or type casting fails), it blocks subsequent steps
// in the chain from running (even RunForce steps).
// It returns the LogicRun instance for method chaining.
func (run LogicRun) RunOrBlock[D any](logic func(D) errs.Err) LogicRun {
	index := run.index
	run.index = index + 1

	switch run.errAt {
	case logicErrAtRun:
		if len(run.errors) == 0 {
			data, ok := run.dataAcc.dataHub.(D)
			if !ok {
				fromType := typeNameOf(run.dataAcc.dataHub)
				toType := typeNameOfTypeParam[D]()
				err := errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
				run.errors = append(run.errors, ErrEntry{
					Index: index,
					Name:  fmt.Sprintf("LogicRun#RunOrBlock(logic-%d)", index),
					Err:   err,
				})
			} else {
				if err := logic(data); err.IsNotOk() {
					run.errors = append(run.errors, ErrEntry{
						Index: index,
						Name:  fmt.Sprintf("LogicRun#RunOrBlock(logic-%d)", index),
						Err:   err,
					})
					run.errAt = logicErrAtBlock
				}
			}
		}
	}
	return run
}

// End finishes the execution chain, cleans up resources if not nested, and returns an error
// indicating any failure that occurred during execution.
func (run LogicRun) End() errs.Err {
	if !run.nested {
		run.dataAcc.end()
	}

	switch run.errAt {
	case logicErrAtBegin:
		return run.errors[0].Err
	default:
		if len(run.errors) == 0 {
			return errs.Ok()
		} else {
			return errs.New(FailToRunLogic{Errors: run.errors})
		}
	}
}
