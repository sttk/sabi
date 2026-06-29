// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
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
	listUnready []dataSrcContainer
	listReady   []dataSrcContainer
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

func (mgr *dataSrcManager) remove(name string) {
	for i := range mgr.listReady {
		if mgr.listReady[i].name == name && mgr.listReady[i].ds != nil {
			mgr.listReady[i].ds.Close()
			mgr.listReady[i].ds = nil
		}
	}
	for i := range mgr.listUnready {
		if mgr.listUnready[i].name == name && mgr.listUnready[i].ds != nil {
			mgr.listUnready[i].ds = nil
		}
	}
}

func (mgr *dataSrcManager) close() {
	for i := len(mgr.listReady) - 1; i >= 0; i-- {
		if mgr.listReady[i].ds != nil {
			mgr.listReady[i].ds.Close()
			mgr.listReady[i].ds = nil
		}
	}
	for i := range mgr.listUnready {
		if mgr.listUnready[i].ds != nil {
			mgr.listUnready[i].ds = nil
		}
	}
	mgr.listReady = nil
	mgr.listUnready = nil
}

func (mgr *dataSrcManager) setup() []ErrEntry {
	if len(mgr.listUnready) == 0 {
		return nil
	}

	ag := AsyncGroup{}
	ii := 0
	nDone := 0
	for i := range mgr.listUnready {
		if mgr.listUnready[i].ds == nil {
			continue
		}
		ag._name = mgr.listUnready[i].name
		ag._index = ii
		ii++
		if err := mgr.listUnready[i].ds.Setup(&ag); err.IsNotOk() {
			ag.addErr(ag._index, ag._name, err)
			nDone = i
			break
		}
	}
	errors := ag.join()

	if len(errors) == 0 {
		for i := range mgr.listUnready {
			if mgr.listUnready[i].ds != nil {
				mgr.listReady = append(mgr.listReady, mgr.listUnready[i])
			}
		}
		mgr.listUnready = nil
		return nil
	} else {
		for i := nDone - 1; i >= 0; i-- {
			if mgr.listUnready[i].ds != nil {
				mgr.listUnready[i].ds.Close()
			}
		}
		return errors
	}
}

func (mgr *dataSrcManager) setupWithOrder(names []string) []ErrEntry {
	if len(mgr.listUnready) == 0 {
		return nil
	}

	indexedMap := make(map[string]int, len(names))
	// Becuase earlier ones take precedence when names overlap
	for i := len(names) - 1; i >= 0; i-- {
		indexedMap[names[i]] = i
	}

	const offsetAvoidingUnset = 1 // To distinguish from the unset value 0.

	orderedIndexes := make([]int, len(names), len(mgr.listUnready))

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

	ag := AsyncGroup{}
	ii := 0
	nDone := 0
	for orderIndex, listIndexPlusOffset := range orderedIndexes {
		if listIndexPlusOffset <= 0 { // Ignore unset
			continue
		}
		listIndex := listIndexPlusOffset - offsetAvoidingUnset
		if mgr.listUnready[listIndex].ds == nil {
			continue
		}
		ag._name = mgr.listUnready[listIndex].name
		ag._index = ii
		ii++
		if err := mgr.listUnready[listIndex].ds.Setup(&ag); err.IsNotOk() {
			ag.addErr(ag._index, ag._name, err)
			nDone = orderIndex
			break
		}
	}
	errors := ag.join()

	if len(errors) == 0 {
		for _, listIndexPlusOffset := range orderedIndexes {
			if listIndexPlusOffset <= 0 { // Ignore unset
				continue
			}
			listIndex := listIndexPlusOffset - offsetAvoidingUnset
			if mgr.listUnready[listIndex].ds == nil {
				continue
			}
			mgr.listReady = append(mgr.listReady, mgr.listUnready[listIndex])
		}
		mgr.listUnready = nil
		return nil
	} else {
		for orderIndex := nDone - 1; orderIndex >= 0; orderIndex-- {
			listIndexPlusOffset := orderedIndexes[orderIndex]
			if listIndexPlusOffset > 0 { // Ignore unset
				listIndex := listIndexPlusOffset - offsetAvoidingUnset
				if mgr.listUnready[listIndex].ds != nil {
					mgr.listUnready[listIndex].ds.Close()
				}
			}
		}
		return errors
	}
}

func (mgr *dataSrcManager) copyDsReadyToMap(contMap map[string]dataSrcContainer) {
	for i := range mgr.listReady {
		contPtr := &mgr.listReady[i]
		contMap[contPtr.name] = *contPtr
	}
}
