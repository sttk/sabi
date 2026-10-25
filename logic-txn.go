// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"fmt"

	"github.com/sttk/errs"
)

// LogicTxn coordinates the execution of multiple business logic functions within a transaction
// in a sequential method chain.
type LogicTxn struct {
	dataAcc *DataAcc
	errAt   logicErrAt
	errors  []ErrEntry
	index   int
}

func newLogicTxn(da *DataAcc) *LogicTxn {
	if err := da.begin(); err.IsNotOk() {
		return &LogicTxn{
			dataAcc: da,
			errAt:   logicErrAtBegin,
			errors:  []ErrEntry{{Err: err}},
			index:   0,
		}
	} else {
		return &LogicTxn{
			dataAcc: da,
			errAt:   logicErrAtRun,
			errors:  []ErrEntry{},
			index:   0,
		}
	}
}

// Run executes the given business logic function if no errors have occurred so far in the
// transaction chain. If an error has already occurred in a preceding step, this logic is
// skipped.
// It returns the LogicTxn instance for method chaining.
func (run *LogicTxn) Run[D any](logic func(D) errs.Err) *LogicTxn {
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
					Name:  fmt.Sprintf("LogicTxn#Run(logic-%d)", index),
					Err:   err,
				})
			} else {
				if err := logic(data); err.IsNotOk() {
					run.errors = append(run.errors, ErrEntry{
						Index: index,
						Name:  fmt.Sprintf("LogicTxn#Run(logic-%d)", index),
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
// It returns the LogicTxn instance for method chaining.
func (run *LogicTxn) RunForce[D any](logic func(D) errs.Err) *LogicTxn {
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
				Name:  fmt.Sprintf("LogicTxn#RunForce(logic-%d)", index),
				Err:   err,
			})
		} else {
			if err := logic(data); err.IsNotOk() {
				run.errors = append(run.errors, ErrEntry{
					Index: index,
					Name:  fmt.Sprintf("LogicTxn#RunForce(logic-%d)", index),
					Err:   err,
				})
			}
		}
	}
	return run
}

// RunOrBlock executes the given business logic function if no errors have occurred so far in
// the transaction chain. If this logic function fails (or type-casting fails), it blocks
// subsequent steps in the chain from running (even RunForce steps).
// It returns the LogicTxn instance for method chaining.
func (run *LogicTxn) RunOrBlock[D any](logic func(D) errs.Err) *LogicTxn {
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
					Name:  fmt.Sprintf("LogicTxn#RunOrBlock(logic-%d)", index),
					Err:   err,
				})
			} else {
				if err := logic(data); err.IsNotOk() {
					run.errors = append(run.errors, ErrEntry{
						Index: index,
						Name:  fmt.Sprintf("LogicTxn#RunOrBlock(logic-%d)", index),
						Err:   err,
					})
					run.errAt = logicErrAtBlock
				}
			}
		}
	}
	return run
}

// EndTxn concludes the transaction chain. If all logic steps succeeded, it commits all data
// connections; if commit fails or if any logic step failed, it rolls back all data
// connections.
// It releases active connection resources and returns an error if any logic or commit step
// failed.
func (run *LogicTxn) EndTxn() errs.Err {
	switch run.errAt {
	case logicErrAtBegin:
		run.dataAcc.end()
		return run.errors[0].Err
	default:
		reports := run.dataAcc.dataConnManager.newFailureReports()
		if len(run.errors) == 0 {
			err := run.dataAcc.dataConnManager.commit(reports)
			if err.IsNotOk() {
				run.dataAcc.dataConnManager.rollback(reports)
			}
			run.dataAcc.end()
			return err
		} else {
			run.dataAcc.dataConnManager.rollback(reports)
			run.dataAcc.end()
			return errs.New(FailToRunLogic{Errors: run.errors})
		}
	}
}
