// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToSetupGlobalDataSrcs struct {
		Errors []ErrEntry
	}
)

var (
	globalDataSrcManager dataSrcManager = newDataSrcManager(false)
	globalDataSrcsFixed  bool           = false
)

func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed {
		globalDataSrcManager.add(name, ds)
	}
}

func Setup() errs.Err {
	if !globalDataSrcsFixed {
		globalDataSrcsFixed = true

		errors := globalDataSrcManager.setup()
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

func SetupWithOrder(names ...string) errs.Err {
	if !globalDataSrcsFixed {
		globalDataSrcsFixed = true

		errors := globalDataSrcManager.setupWithOrder(names)
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

func Shutdown() {
	globalDataSrcManager.close()
}
