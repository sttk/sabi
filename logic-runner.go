// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToRunLogic represents an error reason indicating that one or more logic functions
	// executed in a LogicRun or LogicTxn sequence failed. It wraps the slice of individual
	// errors encountered during execution.
	FailToRunLogic struct {
		// Errors is a slice of ErrEntry containing errors encountered during logic
		// execution.
		Errors []ErrEntry
	}
)

// LogicRunner coordinates the execution of business logic and transactions. It holds a DataAcc
// and its associated DataHub object, manages local data sources, and executes logic functions
// either non-transactionally or within transactions.
type LogicRunner struct {
	dataAcc *DataAcc
}

// NewLogicRunner creates and initializes a new LogicRunner instance using the specified
// NewDataHub constructor function.
func NewLogicRunner(newDataHub NewDataHub) LogicRunner {
	acc := newDataAcc()
	hub := newDataHub(acc)
	acc.dataHub = hub
	return LogicRunner{dataAcc: acc}
}

// NewLogicRunnerWithCommitOrder creates and initializes a new LogicRunner instance with an
// explicit commit order for data connections, specified by their data source names.
func NewLogicRunnerWithCommitOrder(newDataHub NewDataHub, names ...string) LogicRunner {
	acc := newDataAccWithCommitOrder(names)
	hub := newDataHub(acc)
	acc.dataHub = hub
	return LogicRunner{dataAcc: acc}
}

// Close releases all resources, connections, and local data sources managed by this
// LogicRunner.
func (runner *LogicRunner) Close() {
	runner.dataAcc.close()
}

// Uses registers a local data source with a unique identifier specifically for this LogicRunner
// instance. This data source is only visible within this runner's execution scope and is
// initialized when logic begins.
func (runner *LogicRunner) Uses(name string, ds DataSrc) {
	runner.dataAcc.uses(name, ds)
}

// Disuses removes a registered local data source by name from this LogicRunner instance.
func (runner *LogicRunner) Disuses(name string) {
	runner.dataAcc.disuses(name)
}

// Run executes a non-transactional business logic function using this LogicRunner.
// It initializes local data sources before running the logic and cleans up connections upon
// completion. It returns an error if initialization, type-casting to D, or the logic function
// fails.
func (runner *LogicRunner) Run[D any](logic func(D) errs.Err) errs.Err {
	data, ok := runner.dataAcc.dataHub.(D)
	if !ok {
		fromType := typeNameOf(runner.dataAcc.dataHub)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	err := runner.dataAcc.begin()
	if err.IsNotOk() {
		return err
	}
	defer runner.dataAcc.end()

	return logic(data)
}

// Start begins a method chain for executing multiple non-transactional business logic functions
// in sequence using LogicRun.
func (runner *LogicRunner) Start() *LogicRun {
	return newLogicRun(runner.dataAcc, false)
}

// Txn executes a transactional business logic function using this LogicRunner.
// It initializes local data sources, executes the logic function, and automatically commits all
// participating connections if the logic succeeds, or rolls them back if an error occurs.
// It returns an error if initialization, type-casting to D, the logic function, or commit
// fails.
func (runner *LogicRunner) Txn[D any](logic func(D) errs.Err) errs.Err {
	data, ok := runner.dataAcc.dataHub.(D)
	if !ok {
		fromType := typeNameOf(runner.dataAcc.dataHub)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	err := runner.dataAcc.begin()
	if err.IsNotOk() {
		return err
	}
	defer runner.dataAcc.end()

	err = logic(data)
	return runner.dataAcc.commitOrRollback(err)
}

// BeginTxn begins a method chain for executing multiple business logic functions within a
// transaction using LogicTxn.
func (runner *LogicRunner) BeginTxn() *LogicTxn {
	return newLogicTxn(runner.dataAcc)
}
