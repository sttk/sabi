# [Sabi][repo-url] [![Go Reference][pkg-dev-img]][pkg-dev-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

A small framework for Go designed to separate logic from data access.

It achieves this by connecting the logic layer and the data access layer via interfaces, similar to traditional Dependency Injection (DI).
This reduces the dependency between the two, allowing them to be implemented and tested independently.

However, traditional DI often presented inconvenience in how methods were grouped.
Typically, methods were grouped by external data service like a database or by database table.
This meant the logic layer had to depend on units defined by the data access layer's concerns.
Furthermore, such interfaces often contained more methods than a specific piece of logic needed, making it
difficult to tell which methods were actually used in the logic without tracing the code.

This framework addresses that inconvenience.
The data access interface used by a logic function is unique to that specific logic, passed as an argument
to the logic function.
This interface declares all the data access methods that specific logic will use.

On the data access layer side, implementations can be provided by concrete types that fulfill multiple `DataAcc` derived structs.
This allows for implementation in any arbitrary unit — whether by external data service, by table,
or by functional concern.

This is achieved through the following mechanism:

* A `DataHub` struct aggregates all data access methods.
  `DataAcc` derived structs are attached to `DataHub`, giving `DataHub` the implementations of
  the data access methods.
* Logic functions accept specific, narrowly defined data access interfaces as arguments.
  These interfaces declare only the methods relevant to that particular piece of logic.
* The `DataHub` type implements all of these specific data access interfaces. When a `DataHub`
  instance is passed to a logic function, the logic function interacts with it via the narrower
  interface, ensuring it only sees and uses the methods it needs. 
  By embedding mechanism of Go, an interface is satisfied all its methods by other structs that
  implements them.
  The `DataHub` simply needs to have methods that match the signatures of all the methods declared
  across the various logic-facing data access interfaces.

This approach provides strong compile-time guarantees that logic only uses what it declares, while
allowing flexible organization of data access implementations.

## Installation

```go
import (
    "github.com/sttk/sabi"
    "github.com/sttk/errs"
)
```

## Usage

### 1. Implementing `DataSrc` and `DataConn`

First, you'll define `DataSrc` which manages connections to external data services and creates
`DataConn`.
Then, you'll define `DataConn` which represents a session-specific connection and implements
transactional operations.

```go
type FooDataSrc struct {}
func (ds *FooDataSrc) Setup(ag *sabi.AsyncGroup) { return errs.Ok() }
func (ds *FooDataSrc) Close() {}
func (ds *FooDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) { return FooDataConn{}, errs.Ok() }

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
func (ds *BarDataSrc) CreateDataConn() (sabi.DataConn, errs.Err) { return &BarDataConn{}, errs.Ok() }

type BarDataConn struct {}
func (conn *BarDataConn) Commit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
func (conn *BarDataConn) PreCommit(ag *sabi.AsyncGroup) errs.Err { return errs.Ok() }
func (conn *BarDataConn) PostCommit(ag *sabi.AsyncGroup) {}
func (conn *BarDataConn) ShouldForceBack() bool { return false }
func (conn *BarDataConn) Rollback(ag *sabi.AsyncGroup) {}
func (conn *BarDataConn) ForceBack(ag *sabi.AsyncGroup) {}
func (conn *BarDataConn) Close() {}
```

### 2. Implementing logic functions and data traits

Define interfaces and functions that express your application logic.
These interfaces are independent of specific data source implementations, improving testability.

```go
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
```

### 3. Implementing `DataAcc` derived structs

The `DataAcc` interface abstracts access to data connections.
The methods defined here will be used to obtain data connections via `DataHub` and perform
actual data operations.

```go
type GettingDataAcc struct { sabi.DataAcc }
func (data *GettingDataAcc) GetText() (string, errs.Err) {
	conn, err := sabi.GetDataConn[*FooDataConn](data, "foo")
	return "output text"
}

type SettingDataAcc struct { sabi.DataAcc }
func (data *SettingDataAcc) SetText(text string) errs.Err {
	conn, err := sabi.GetDataConn[*BarDataConn](data, "bar")
  if err != nil { return err }
	return errs.Ok()
}
```

### 4. Integrating data interfaces and `DataAcc` derived structs into `DataHub`

The `DataHub` is the central component that manages all `DataSrc` and `DataConn`,
providing access to them for your application logic.
By implementing the data interface (`MyData`) from step 2 and the `DataAcc` structs
from step 3 on `DataHub`, you integrate them.

```go
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
```

### 5. Using logic functions and `DataHub`

Inside your `init` function, register your global `DataSrc`.
Next, `main` function calls `run` function, and inside `run` function, setup the `sabi` framework.
Then, create an instance of `DataHub` and register the necessary local `DataSrc` using
the `uses` method.
Finally, use the `txn` method of `DataHub` to execute your defined application logic
function (`my_logic`) within a transaction.
This automatically handles transaction commits and rollbacks.

```go
func init() {
	// Register global DataSrc.
	sabi.Uses("foo", &FooDataSrc{})
}

func main() {
	if run().IsNotOk() {
		os.Exit(1)
	}
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
```

## Supporting Go versions

This framework supports Go 1.21 or later.

### Actual test results for each Go version:

```
% gvm-fav
Now using version go1.21.13
go version go1.21.13 darwin/amd64
ok  	github.com/sttk/sabi	8.749s	coverage: 96.8% of statements

Now using version go1.22.12
go version go1.22.12 darwin/amd64
ok  	github.com/sttk/sabi	8.747s	coverage: 96.8% of statements

Now using version go1.23.10
go version go1.23.10 darwin/amd64
ok  	github.com/sttk/sabi	8.737s	coverage: 96.8% of statements

Now using version go1.24.4
go version go1.24.4 darwin/amd64
ok  	github.com/sttk/sabi	8.753s	coverage: 96.8% of statements

Back to go1.24.4
Now using version go1.24.4
```

## License

Copyright (C) 2022-2025 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi
[pkg-dev-img]: https://pkg.go.dev/badge/github.com/sttk/sabi.svg
[pkg-dev-url]: https://pkg.go.dev/github.com/sttk/sabi
[ci-img]: https://github.com/sttk/sabi/actions/workflows/go.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi/actions
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
