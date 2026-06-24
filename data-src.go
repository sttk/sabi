// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type DataSrcErr struct {
	Name string
	Err  errs.Err
}

type DataSrc interface {
	Setup(ag *AsyncGroup) errs.Err
	Close()
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

func (mgr *dataSrcManager) setup() []DataSrcErr {
	if len(mgr.listUnready) == 0 {
		return nil
	}

	ag := AsyncGroup{}
	for i := range mgr.listUnready {
		if mgr.listUnready[i].ds != nil {
			ag._index = i
			if err := mgr.listUnready[i].ds.Setup(&ag); err.IsNotOk() {
				ag.addErr(ag._index, err)
				break
			}
		}
	}
	nDone := ag._index
	indexedErrors := ag.join()

	if len(indexedErrors) == 0 {
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
		errors := make([]DataSrcErr, len(indexedErrors))
		for i, idxErr := range indexedErrors {
			errors[i].Name = mgr.listUnready[idxErr.Index].name
			errors[i].Err = idxErr.Err
		}
		return errors
	}
}

func (mgr *dataSrcManager) setupWithOrder(names []string) []DataSrcErr {
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
	nDone := 0
	for orderIndex, listIndexPlusOffset := range orderedIndexes {
		if listIndexPlusOffset > 0 { // Ignore unset
			listIndex := listIndexPlusOffset - offsetAvoidingUnset
			if mgr.listUnready[listIndex].ds != nil {
				ag._index = listIndex
				if err := mgr.listUnready[listIndex].ds.Setup(&ag); err.IsNotOk() {
					ag.addErr(ag._index, err)
					nDone = orderIndex
					break
				}
			}
		}
	}
	indexedErrors := ag.join()

	if len(indexedErrors) == 0 {
		for _, listIndexPlusOffset := range orderedIndexes {
			if listIndexPlusOffset > 0 { // Ignore unset
				listIndex := listIndexPlusOffset - offsetAvoidingUnset
				if mgr.listUnready[listIndex].ds != nil {
					mgr.listReady = append(mgr.listReady, mgr.listUnready[listIndex])
				}
			}
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
		errors := make([]DataSrcErr, len(indexedErrors))
		for i, idxErr := range indexedErrors {
			errors[i].Name = mgr.listUnready[idxErr.Index].name
			errors[i].Err = idxErr.Err
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
