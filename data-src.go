// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"slices"

	"github.com/sttk/errs"
)

// DataSrc is an interface representing a factory or connection pool for data sources
// (such as databases, external APIs, or files) that need initialization and cleanup.
// It manages the lifecycle of the data source before connections are created, and
// provides a way to instantiate connections for transactions.
type DataSrc interface {
	// Setup initializes the data source, such as establishing connection pools or
	// loading configurations. It accepts a pointer to an AsyncGroup to facilitate
	// asynchronous setup tasks in concurrent scenarios. It returns an error if
	// the initialization fails.
	Setup(ag *AsyncGroup) errs.Err

	// Close releases any resources, connections, or files held by the data source
	// during its shutdown phase, preventing resource leaks.
	Close()

	// CreateDataConn instantiates and returns a new DataConn, which represents a
	// database connection or session that can participate in transactions.
	// It returns the connection and an error if the connection cannot be created.
	CreateDataConn() (DataConn, errs.Err)
}

type dataSrcContainer struct {
	local bool
	name  string
	ds    DataSrc
}

type dataSrcManager struct {
	local       bool
	listUnready []dataSrcContainer // Elements whose .ds is nil are possible.
	listReady   []dataSrcContainer // Elements whose .ds is nil are impossible.
}

func newDataSrcManager(local bool) dataSrcManager {
	return dataSrcManager{
		local:       local,
		listUnready: make([]dataSrcContainer, 0),
		listReady:   make([]dataSrcContainer, 0),
	}
}

func (mgr *dataSrcManager) add(name string, ds DataSrc) {
	mgr.listUnready = append(mgr.listUnready, dataSrcContainer{local: mgr.local, name: name, ds: ds})
}

func closeDataSrcSafely(ds DataSrc) (recovered any) {
	if ds == nil {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			recovered = r
		}
	}()

	ds.Close()
	return nil
}

func (mgr *dataSrcManager) remove(name string) {
	var firstPanic any

	mgr.listReady = slices.DeleteFunc(mgr.listReady, func(cont dataSrcContainer) bool {
		if cont.name == name {
			if p := closeDataSrcSafely(cont.ds); p != nil && firstPanic == nil {
				firstPanic = p
			}
			return true
		} else {
			return false
		}
	})
	mgr.listUnready = slices.DeleteFunc(mgr.listUnready, func(cont dataSrcContainer) bool {
		return (cont.name == name || cont.ds == nil)
	})

	if firstPanic != nil {
		panic(firstPanic)
	}
}

func (mgr *dataSrcManager) close() {
	var firstPanic any

	for i := len(mgr.listReady) - 1; i >= 0; i-- {
		if p := closeDataSrcSafely(mgr.listReady[i].ds); p != nil && firstPanic == nil {
			firstPanic = p
		}
	}
	mgr.listReady = nil
	mgr.listUnready = nil

	if firstPanic != nil {
		panic(firstPanic)
	}
}

func (mgr *dataSrcManager) setup() (errors []ErrEntry) {
	if len(mgr.listUnready) == 0 {
		return nil
	}

	nDone := 0
	var firstPanic any

	defer func() {
		if r := recover(); r != nil && firstPanic == nil {
			firstPanic = r
		}
		if len(errors) > 0 {
			for i := nDone - 1; i >= 0; i-- {
				if p := closeDataSrcSafely(mgr.listUnready[i].ds); p != nil && firstPanic == nil {
					firstPanic = p
				}
			}
		}
		if firstPanic != nil {
			panic(firstPanic)
		}
	}()

	ag := AsyncGroup{}

	for i := range mgr.listUnready {
		if mgr.listUnready[i].ds == nil {
			continue
		}
		ag.name = mgr.listUnready[i].name
		ag.index = i
		nDone = i + 1
		if err := mgr.listUnready[i].ds.Setup(&ag); err.IsNotOk() {
			ag.addErr(ag.index, ag.name, err)
			break
		}
	}

	errors = ag.join()
	if len(errors) > 0 {
		return errors
	}

	for i := range mgr.listUnready {
		if mgr.listUnready[i].ds != nil {
			mgr.listReady = append(mgr.listReady, mgr.listUnready[i])
		}
	}
	mgr.listUnready = nil
	return nil
}

func (mgr *dataSrcManager) setupWithOrder(names []string) (errors []ErrEntry) {
	if len(mgr.listUnready) == 0 {
		return nil
	}

	indexedMap := make(map[string]int, len(names))
	// Because earlier ones take precedence when names overlap
	for i := len(names) - 1; i >= 0; i-- {
		indexedMap[names[i]] = i
	}

	const offsetAvoidingUnset = 1 // To distinguish from the unset value 0.

	orderedIndexes := make([]int, len(names), max(len(names), len(mgr.listUnready)))

	for listIndex := range mgr.listUnready {
		if mgr.listUnready[listIndex].ds != nil {
			name := mgr.listUnready[listIndex].name
			if orderIndex, ok := indexedMap[name]; ok {
				orderedIndexes[orderIndex] = listIndex + offsetAvoidingUnset
				delete(indexedMap, name)
			} else {
				orderedIndexes = append(orderedIndexes, listIndex+offsetAvoidingUnset)
			}
		}
	}

	nDone := 0
	var firstPanic any

	defer func() {
		if r := recover(); r != nil && firstPanic == nil {
			firstPanic = r
		}
		if len(errors) > 0 {
			for orderIndex := nDone - 1; orderIndex >= 0; orderIndex-- {
				listIndexPlusOffset := orderedIndexes[orderIndex]
				if listIndexPlusOffset > 0 { // Ignore unset
					listIndex := listIndexPlusOffset - offsetAvoidingUnset
					//if mgr.listUnready[listIndex].ds == nil { // impossible
					//  continue
					//}
					if p := closeDataSrcSafely(mgr.listUnready[listIndex].ds); p != nil && firstPanic == nil {
						firstPanic = p
					}
				}
			}
		}
		if firstPanic != nil {
			panic(firstPanic)
		}
	}()

	ag := AsyncGroup{}
	for orderIndex, listIndexPlusOffset := range orderedIndexes {
		if listIndexPlusOffset <= 0 { // Ignore unset
			continue
		}
		listIndex := listIndexPlusOffset - offsetAvoidingUnset
		//if mgr.listUnready[listIndex].ds == nil { // impossible
		//	continue
		//}
		ag.name = mgr.listUnready[listIndex].name
		ag.index = orderIndex
		nDone = orderIndex + 1
		if err := mgr.listUnready[listIndex].ds.Setup(&ag); err.IsNotOk() {
			ag.addErr(ag.index, ag.name, err)
			break
		}
	}
	errors = ag.join()

	if len(errors) > 0 {
		return errors
	}

	for _, listIndexPlusOffset := range orderedIndexes {
		if listIndexPlusOffset <= 0 { // Ignore unset
			continue
		}
		listIndex := listIndexPlusOffset - offsetAvoidingUnset
		//if mgr.listUnready[listIndex].ds == nil { // impossible
		//	continue
		//}
		mgr.listReady = append(mgr.listReady, mgr.listUnready[listIndex])
	}
	mgr.listUnready = nil
	return nil
}

func (mgr *dataSrcManager) copyDsReadyToMap(contMap map[string]dataSrcContainer) {
	for i := range mgr.listReady {
		contPtr := &mgr.listReady[i]
		contMap[contPtr.name] = *contPtr
	}
}
