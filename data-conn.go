// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

// The interface that abstracts a connection per session to an external data service,
// such as a database, file system, or messaging service.
//
// Its primary purpose is to enable cohesive transaction operations across multiple
// external data services within a single transaction context. Implementations of this
// interface provide the concrete input/output operations for their respective data services.
//
// Methods declared within this interface are designed to handle transactional logic.
// The AsyncGroup parameter in various methods allows for asynchronous processing
// when commit or rollback operations are time-consuming.
type DataConn interface {
	// Attempts to commit the changes made within this data connection's transaction.
	//
	// This method should encapsulate the logic required to finalize the transaction
	// for the specific external data service.
	Commit(ag *AsyncGroup) errs.Err

	// This method is executed before the transaction commit process for all DataConn instances
	// involved in the transaction.
	//
	// This method provides a timing to execute unusual commit processes or update operations not
	// supported by transactions beforehand.
	// This allows other update operations to be rolled back if the operations in this method
	// fail.
	PreCommit(ag *AsyncGroup) errs.Err

	// This method is executed after the transaction commit process has successfully completed
	// for all DataConn instances involved in the transaction.
	//
	// It provides a moment to perform follow-up actions that depend on a successful commit.
	// For example, after a database commit, a messaging service's DataConn might use this
	// method to send a "transaction completed" message.
	PostCommit(ag *AsyncGroup)

	// Determines whether a "force back" operation is required for this data connection.
	//
	// A force back is typically executed if one external data service successfully commits
	// its changes, but a subsequent external data service within the same transaction fails
	// its commit. This method indicates if the committed changes of *this* data service
	// need to be undone (forced back).
	ShouldForceBack() bool

	// Rolls back any changes made within this data connection's transaction.
	//
	// This method undoes all operations performed since the beginning of the transaction,
	// restoring the data service to its state before the transaction began.
	Rollback(ag *AsyncGroup)

	// Executes an operation to revert committed changes.
	//
	// This method provides an opportunity to undo changes that were successfully committed
	// to this external data service, typically when a commit fails for *another* data service
	// within the same distributed transaction, necessitating a rollback of already committed
	// changes.
	ForceBack(ag *AsyncGroup)

	// Closes the connection to the external data service.
	//
	// This method should release any resources held by the data connection, ensuring
	// a graceful shutdown of the connection.
	Close()
}

type dataConnContainer struct {
	prev *dataConnContainer
	next *dataConnContainer
	name string
	conn DataConn
}

type dataConnList struct {
	head *dataConnContainer
	last *dataConnContainer
}

func (list *dataConnList) appendContainer(ptr *dataConnContainer) {
	ptr.next = nil

	if list.last == nil {
		list.head = ptr
		list.last = ptr
		ptr.prev = nil
	} else {
		list.last.next = ptr
		ptr.prev = list.last
		list.last = ptr
	}
}

func (list *dataConnList) closeDataConns() {
	ptr := list.last
	for ptr != nil {
		ptr.conn.Close()
		ptr = ptr.prev
	}
	list.head = nil
	list.last = nil
}
