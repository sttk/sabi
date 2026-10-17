package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToRunLogic struct {
		Errors []ErrEntry
	}
)

type LogicRunner struct {
	dataAcc *DataAcc
}

func NewLogicRunner(newDataHub NewDataHub) LogicRunner {
	acc := newDataAcc()
	hub := newDataHub(acc)
	acc.dataHub = hub
	return LogicRunner{dataAcc: acc}
}

func NewLogicRunnerWithCommitOrder(newDataHub NewDataHub, names ...string) LogicRunner {
	acc := newDataAccWithCommitOrder(names)
	hub := newDataHub(acc)
	acc.dataHub = hub
	return LogicRunner{dataAcc: acc}
}

func (runner *LogicRunner) Close() {
	runner.dataAcc.close()
}

func (runner *LogicRunner) Uses(name string, ds DataSrc) {
	runner.dataAcc.uses(name, ds)
}

func (runner *LogicRunner) Disuses(name string) {
	runner.dataAcc.disuses(name)
}

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

func (runner *LogicRunner) Start() LogicRun {
	return newLogicRun(runner.dataAcc, false)
}

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

func (runner *LogicRunner) BeginTxn() LogicTxn {
	return newLogicTxn(runner.dataAcc)
}
