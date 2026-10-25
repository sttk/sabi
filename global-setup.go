// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"sync/atomic"

	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToSetupGlobalDataSrcs represents an error reason indicating that one or more
	// global data sources failed to initialize during their setup phase. It wraps the
	// list of individual errors encountered by the data sources.
	FailToSetupGlobalDataSrcs struct {
		// Errors is a slice of ErrEntry containing errors encountered during setup.
		Errors []ErrEntry
	}
)

var (
	globalDataSrcManager dataSrcManager = newDataSrcManager(false)
	globalDataSrcsFixed  atomic.Bool
)

// Uses registers a global data source with a unique identifier. This registration must occur
// before Setup is called, as global data sources are initialized during the Setup phase and
// shared across LogicRunner and DataAcc instances.
func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed.Load() {
		globalDataSrcManager.add(name, ds)
	}
}

// Setup initializes all registered global data sources. It locks the global data sources to
// prevent further registrations. If any data source setup fails, it shuts down all successfully
// initialized data sources and returns an error wrapper.
func Setup() errs.Err {
	if globalDataSrcsFixed.CompareAndSwap(false, true) {
		errors := globalDataSrcManager.setup()
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

// SetupWithOrder initializes all registered global data sources in the specific order defined
// by the provided names. Data sources not specified in the list are initialized after the
// ordered ones.
// If initialization fails, it shuts down all successfully initialized data sources and returns
// an error wrapper.
func SetupWithOrder(names ...string) errs.Err {
	if globalDataSrcsFixed.CompareAndSwap(false, true) {
		errors := globalDataSrcManager.setupWithOrder(names)
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

// Shutdown cleans up and closes all global data sources that were successfully initialized,
// releasing resources like connection pools.
func Shutdown() {
	globalDataSrcManager.close()
}
