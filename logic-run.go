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
