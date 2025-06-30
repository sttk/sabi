// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

/*
Package sabi provides a small framework for Go, designed to separate application logic
from data access.

In this framework, the logic exclusively takes a data access interface as its argument,
and all necessary data access is defined by a single data access interface.
Conversely, the concrete implementations of data access methods are provided as methods
of DataAcc derived struct, allowing for flexible grouping, often by data service.

The DataHub bridges these two parts.
Since the DataHub derived struct embeds all DataAcc derived structs, it can provide all data
access methods. Then, by type assertion from the data hub to the interface which is only one
argument of a logic function, the logic function can perform the necessary data access through
that interface, while also being prevented from performing any other data access.

Furthermore, the DataHub provides transaction control for data operations performed
within the logic.
You can execute logic functions with transaction control using the DataHub#txn method,
or without it using the DataHub#run method.
This framework brings clear separation and robustness to Go application design.

# Example

The following is a sample code using this framework

	import (
		"os"

		"github.com/sttk/sabi"
		"github.com/sttk/errs"
	)

	// (1) Implements DataSrc(s) and DataConn(s).

	type FooDataSrc struct {}
	func (ds *FooDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (ds *FooDataSrc) Close() {}
	func (ds *FooDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) { return &FooDataConn{}, errs.Ok() }

	type FooDataConn struct {}
	func (conn *FooDataConn) Commit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (conn *FooDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (conn *FooDataConn) PostCommit(ag *sabi.AsyncGroup) {}
	func (conn *FooDataConn) ShouldForceBack() bool { return false }
	func (conn *FooDataConn) Rollback(ag *sabi.AsyncGroup) {}
	func (conn *FooDataConn) ForceBack(ag *sabi.AsyncGroup) {}
	func (conn *FooDataConn) Close() {}

	type BarDataSrc struct {}
	func (ds *BarDataSrc) Setup(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (ds *BarDataSrc) Close() {}
	func (ds *BarDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) { return &FooDataConn{}, errs.Ok() }

	type BarDataConn struct {}
	func (conn *BarDataConn) Commit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (conn *BarDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
	func (conn *BarDataConn) PostCommit(ag *sabi.AsyncGroup) {}
	func (conn *BarDataConn) ShouldForceBack() bool { return false }
	func (conn *BarDataConn) Rollback(ag *sabi.AsyncGroup) {}
	func (conn *BarDataConn) ForceBack(ag *sabi.AsyncGroup) {}
	func (conn *BarDataConn) Close() {}

	// (2) Implements logic functions and data interfaces

	type MyData interface {
		GetText() (string, errs.Err)
		SetText(text string) errs.Err
	}

	func MyLogic(data MyData) errs.Err {
		text, err := data.GetText()
		if err != nil {
			return err
		}
		return data.SetText(text)
	}

	// (3) Implements DataAcc(s)

	type GettingDataAcc struct { sabi.DataAcc }
	func (data *GettingDataAcc) GetText() (string, errs.Err) {
		conn, err := GetDataConn[*FooDataConn](data, "foo")
		return "output text"
	}

	type SettingDataAcc struct { sabi.DataAcc }
	func (data *SettingDataAcc) SetText(text string) errs.Err {
		conn, err := GetDataConn[*BarDataConn](data, "bar")
		return errs.Ok()
	}

	// (4) Consolidate data traits and DataAcc traits to a DataHub.

	type MyDataHub struct {
		sabi.DataHub
		*FooDataAcc
		*BarDataAcc
	}
	func NewMyDataHub() sabi.DataHub {
		hub := sabi.NewDataHub()
		return MyDataHub {
			DataHub: hub,
			FooDataAcc: &FooDataAcc{DataAcc: hub},
			BarDataAcc: &BarDataAcc{DataAcc: hub},
		}
	}

	// (5) Use the logic functions and the DataHub

	func init() {
		// Register global DataSrc.
		sabi.Uses("foo", &FooDataSrc{})
	}

	func run() {
		// Set up the sabi framework.
		if err := sabi.Setup(); err != nil { return err }
		defer sabi.Shutdown()

		// Creates a new instance of DataHub.
		data := sabi.NewDataHub()
		// Register session-local DataSrc with DataHub.
		data.Uses("bar", &BarDataSrc{})

		// Execute application logic within a transaction.
		// my_logic performs data operations via DataHub.
		return sabi.Txn(my_logic)
	}

	func main() {
		if run().IsNotOk() {
			os.Exit(1)
		}
	}
*/
package sabi
