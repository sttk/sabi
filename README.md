<div align="center">
  <a href="https://github.com/sttk/sabi">
    <img src="./images/sabi-go.png" width="250" height="auto" alt="Sabi"/>
  </a>

  <h2>
  "sabi" - A small framework to separate logics and data accesses
  </h2>
  <br>

  [![Go Reference][pkg-dev-img]][pkg-dev-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]
</div>

## Overview

**sabi** was developed with the goal of achieving a thorough separation between business logic and data access. However, it sets itself apart from conventional Dependency Injection (DI) frameworks that merely invert dependencies by sandwiching an interface between the two layers.

In particular, two key techniques elevate **sabi** into a highly advanced framework: the introduction of data access interfaces optimized for each specific piece of logic, and an approach that routes inputs and outputs from the controller layer directly into the data access layer via `DataSrc`, bypassing the logic layer entirely.

The former approach fully materializes the **Interface Segregation Principle (ISP)**, which has frequently become a mere formality within the SOLID principles. Furthermore, because adding methods to `DataAcc` or incorporating additional services via `DataSrc` does not affect existing logic that does not utilize them, capabilities can be extended without modifying existing code—thereby conceptually satisfying the **Open-Closed Principle (OCP)**.

Additionally, even if a `DataAcc` implementation providing a specific capability is replaced with an alternative implementation, or if responsibilities are rearranged among different `DataAcc` interfaces within the data access layer, the behavior of the logic layer remains unchanged as long as the underlying contract is maintained. Thus, the **Liskov Substitution Principle (LSP)** is realized at the contractual level.

Moreover, because both high-level logic and low-level data access depend on the `DataAcc` contract defined as an individual capability, the **Dependency Inversion Principle (DIP)** is also achieved at the architectural level. In this manner, rather than adhering strictly to the classical OOP context that presupposes inheritance, **sabi** realizes the core intent of each principle—"separation of concerns," "localization of change impact," "substitutability based on contracts," and "dependence on abstractions"—at the architectural level. It possesses a structure that conceptually aligns with all SOLID principles, an achievement that is exceptionally rare in the history of software design.

The latter approach—which routes inputs and outputs from the controller layer directly into the data access layer, bypassing the logic layer entirely—was conceived by breaking down the role of the controller layer into two distinct elements: "logic invocation" and "input/output data." In conventional architectures, because these two elements remained unseparated, a hierarchical structure dedicated solely to data flow (the so-called "data bucket brigade") was inevitable, requiring data to be transformed as it was passed from one layer to the next. However, by routing input/output data directly to the infrastructure layer (the data access department), this redundant data flow and the very hierarchical structure that supported it become entirely unnecessary.

As a result, the residual hierarchical dependency that typically persists between logic and data access is completely eliminated, elevating them into an equal relationship based on the contract defined by the interface's method signatures. This can be viewed as an evolutionary extension of the Dependency Inversion Principle (DIP)—pushing beyond conventional DIP, which merely inverts the direction of dependency, to minimize and localize the dependency itself within contractual boundaries.

This structure delivers the "restricted dependencies" and "localization of explicitly bounded contexts" required for AI-driven automated programming, making **sabi** a cutting-edge, AI-friendly, Capability-oriented framework for today's AI era.

## Installation

```sh
go get github.com/sttk/sabi
```

## Usage

### 1. Implementing a logic function and a data access interface

First, define a function that represents your application logic, along with its dedicated data access interface.
This interface is independent of specific data source implementations, improving testability.

```go
import "github.com/sttk/errs"

type MyData interface {
    GetText() (string, errs.Err)
    SetText(text string) errs.Err
}

func MyLogic(data MyData) errs.Err {
    text, err := data.GetText()
    if err.IsNotOk() {
        return err
    }
    return data.SetText(text)
}
```

### 2. Implementing DataAcc derived structs

The `DataAcc` interface provides a simple mechanism to retrieve `DataConn` objects.
However, it's the derived structs (like `GettingDataAcc` and `SettingDataAcc` in this example) that define the application-specific methods for accessing data.
These methods then use `GetDataConn` to obtain the appropriate `DataConn` and perform the actual data operations.

```go
import (
  "fmt"
  "os"

  "github.com/sttk/errs"
  "github.com/sttk/sabi"
  "github.com/sttk/sabi_redis"
  "github.com/sttk/sabi_stdio"  // This is a conceptual, non-existent DataConn.
)

type GettingDataAcc struct {
  sabi.DataAcc
}
func (da *GettingDataAcc) GetText() (string, errs.Err) {
    return "output text", errs.Ok()
}

type SettingDataAcc struct {
  sabi.DataAcc
}
func (da *SettingDataAcc) SetText(text string) errs.Err {
  ctx := da.Context()
  dc, err := sabi.GetDataConn[*sabi_redis.RedisDataConn](da, "redis")
  if err.IsNotOk() {
    return err
  }

  redisConn := dc.GetConnection()
  e := redisConn.Set(ctx, "key", text, 0).Err()
  if e != nil {
    return errs.New("fail to set text to key", e)
  }
  dc.AddRollback(func(rConn *redis.Conn) errs.Err {
    e := rConn.Del(ctx, "key").Err()
    return errs.New("fail to delete key for rollback", e)
  })

  stdioConn, err := sabi.GetDataConn[*sabi_stdio.StdioDataConn](da, "stdio")
  if err.IsNotOk() {
    return err
  }
  stdioConn.AddPostCommit(func(_ *os.File, stdout *os.File, _ *os.File) errs.Err {
    fmt.Fprintf(stdout, "%s", text)
    return errs.Ok()
  })

  return errs.Ok()
}
```

### 3. Integrating data interfaces and DataAcc derived structs into DataHub

The `DataHub` is the central component that manages all `DataSrc` and `DataConn`,
providing access to them for your application logic.
By implementing the data interface (`MyData`) from step 1. and the `DataAcc` structs
from step 2. on `DataHub`, you integrate them.

```go
import "github.com/sttk/sabi"

type MyDataHub struct {
    sabi.DataHub
    *GettingDataAcc
    *SettingDataAcc
}

func NewMyDataHub() sabi.DataHub {
    hub := sabi.NewDataHub()
    return MyDataHub {
        DataHub: hub,
        GettingDataAcc: &GettingDataAcc{DataAcc: hub},
        SettingDataAcc: &SettingDataAcc{DataAcc: hub},
    }
}

// Since this statement does not remain in the runtime binary, it is a good idea to include it
// in actual code as a compile-time check to ensure that all methods have been fully implemented.
var _ MyData = (*MyDataHub)(nil)
```

### 4. Using logic functions and DataHub

Inside your `init` function, register your global `DataSrc`.
Next, `main` function calls `run` function, and inside `run` function, set up the `sabi` framework.
Then, create an instance of `DataHub` and register the necessary local `DataSrc` using
the `Uses` method.
Finally, use the `Run` function or `Txn` function to execute your defined application logic
function (`MyLogic`) without or within a transaction.

```go
import (
  "context"

  "github.com/sttk/errs"
  "github.com/sttk/sabi"
)

func init() {
    // Register global DataSrc.
    sabi.Uses("foo", &FooDataSrc{})
}

func main() {
    if run().IsNotOk() {
        os.Exit(1)
    }
}

func run() errs.Err {
    // Set up the sabi framework.
    if err := sabi.Setup(); err != nil {
      return err
    }
    defer sabi.Shutdown()

    // Creates a new instance of DataHub.
    data := sabi.NewMyDataHub()
    defer data.Close()

    // Register session-local DataSrc with DataHub.
    data.Uses("bar", &BarDataSrc{})

    data.SetContext(context.Background())

    // Execute application logic without a transaction control.
    return sabi.Run(data, MyLogic)

    // If you need to execute logic within a transaction, use the `Txn` function instead of `Run`
    // return sabi.Txn(data, MyLogic)
}
```

## Related Links

### Data Sources
- [sabi_redis (Go)](https://github.com/sttk/sabi_redis) ... The DataSrc implementation for Redis

### Implementations in other languages
- [sabi (Rust)](https://github.com/sttk/sabi-rust) ... The sabi implementation in Rust
- [sabi (Java)](https://github.com/sttk/sabi-java) ... The sabi implementation in Java


## Supporting Go versions

This framework supports Go 1.23 or later.

### Actual test results for each Go version:

```sh
% gvm-fav
Now using version go1.23.12
go version go1.23.12 darwin/amd64
ok  	github.com/sttk/sabi	8.564s	coverage: 97.0% of statements

Now using version go1.24.13
go version go1.24.13 darwin/amd64
ok  	github.com/sttk/sabi	8.572s	coverage: 97.0% of statements

Now using version go1.25.8
go version go1.25.8 darwin/amd64
ok  	github.com/sttk/sabi	8.778s	coverage: 97.0% of statements

Now using version go1.26.1
go version go1.26.1 darwin/amd64
ok  	github.com/sttk/sabi	8.739s	coverage: 97.0% of statements

Back to go1.26.1
Now using version go1.26.1
```

## License

Copyright (C) 2022-2026 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi
[pkg-dev-img]: https://pkg.go.dev/badge/github.com/sttk/sabi.svg
[pkg-dev-url]: https://pkg.go.dev/github.com/sttk/sabi
[ci-img]: https://github.com/sttk/sabi/actions/workflows/go.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
