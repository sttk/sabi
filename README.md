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

**sabi** was developed with the goal of thoroughly separating business logic from data access. However, it differs from conventional Dependency Injection (DI) frameworks that merely invert dependencies by placing an interface between the two layers—**sabi** draws a clear line beyond that simple approach.

What elevates **sabi** to an advanced framework in particular are the following two key techniques: introducing a data-access interface optimized for each individual piece of logic, and routing input/output from the controller layer directly to the data access layer via `DataSrc`, completely bypassing the logic layer.

### Introducing Data-Access Traits Optimized Per Logic Unit

The former approach thoroughly embodies the Interface Segregation Principle (ISP)—one of the SOLID principles that has often ended up more nominal than real in practice. Each piece of logic (use case) defines, on the logic side, its own dedicated interface that specifies only the operations it truly needs. Meanwhile, the data access side implements interfaces based on its responsibility as a data provider. The `DataHub` then mediates and maps between the two, so that the logic side never needs to be aware of the data access side's structure, and the data access side never needs to depend on the structure of individual pieces of logic—each maintains its own independent responsibility.

This design is grounded in the philosophy that "the world does not exist as a single, fixed, objective reality, but rather reveals its meaning and form according to the questions and purposes held by the observing subject." The interface that logic should see should not be dictated by the constraints of the data access side, but should instead be defined based on the logic's own context and needs. The same holds true in reverse for the interface that data access should see. What matters to the logic is not the structure of the database or the ORM, but the "capability required to realize this particular use case." What matters to the data access side, on the other hand, is how to access storage or external services. By having both sides define their interfaces according to their own respective contexts, and having the `DataHub` bridge them together, true loose coupling is achieved—one where neither side depends on the other's internal structure.

Furthermore, because logic never needs to know any implementation details of data access, it can easily be swapped out for mocks that provide the necessary capabilities during testing, achieving high testability as well.

### A Structure That Routes Controller-Layer I/O Directly to the Data Access Layer, Bypassing the Logic Layer

The latter approach—routing input/output from the controller layer directly to the data access layer, completely bypassing the logic layer—was conceived by decomposing the controller layer's role into two elements: "invoking logic" and "input/output data." In conventional architectures, because these two elements were never separated, a layered structure whose sole responsibility was data flow (the so-called "data bucket brigade") became unavoidable, forcing data to be transformed every time it crossed a layer. However, by routing input/output data directly to the infrastructure layer (the data access division), this redundant data flow—and the layered structure that existed to support it—becomes entirely unnecessary.

As a result, the hierarchical dependency that would normally remain between logic and data access is completely eliminated, and is elevated instead into an equal relationship based on a contract defined by the interface's method signatures. This can be seen as an evolutionary extension of the Dependency Inversion Principle (DIP)—taking the conventional DIP, which merely reverses the direction of dependency, a step further by minimizing and localizing the dependency itself within the boundary of the contract.

### An AI-Friendly, Capability-Oriented Framework

Moreover, this structure is also well-suited to AI-driven automated programming. For AI-driven automated programming to be performed safely and accurately, the following conditions are required:

* **Localized dependencies**: The scope of dependencies an AI must grasp for a single change should be confined to a narrow portion of the system, not the system as a whole.
* **Explicit boundaries of side effects**: The extent to which side effects can propagate should be made explicit in the code itself, rather than relying on implicit call ordering.
* **Localized impact of change**: Adding features or swapping implementations should not ripple out into existing code that doesn't use them.
* **Substitutability through contract**: An implementation should be safely replaceable as long as it satisfies the contract (interface), without needing to know its implementation details.

**sabi** satisfies all of these conditions at the structural level by defining a dedicated data access interface for each piece of logic and making that logic depend only on that narrow interface—so that adding functionality to a particular `DataAcc` never affects existing logic, and swapping implementations can be done safely as long as the interface's contract is satisfied. In this way, **sabi** is not merely a DI framework, but an AI-friendly, capability-oriented framework suited to the modern AI era.


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
