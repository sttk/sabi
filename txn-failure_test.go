package sabi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sttk/errs"
)

func TestTxnFailureReport(t *testing.T) {
	t.Run("newTxnFailureReport", func(t *testing.T) {
		report := newTxnFailureReport("foo", "FooDataConn")

		assert.Equal(t, report.DataConnName, "foo")
		assert.Equal(t, report.DataConnType, "FooDataConn")
		assert.Equal(t, report.Cause.State, NoneByUncommitted)
		assert.Equal(t, report.Cause.Err, errs.Ok())
		assert.Equal(t, report.Rollback.State, NoneByNotRolledBack)
		assert.Equal(t, report.Rollback.Err, errs.Ok())
	})

	t.Run("IsCauseOfFailure", func(t *testing.T) {
		report := newTxnFailureReport("foo", "FooDataConn")
		assert.Equal(t, report.Cause.State, NoneByUncommitted)
		assert.False(t, report.IsCauseOfFailure())

		report.Cause.State = NoneByCommitted
		assert.False(t, report.IsCauseOfFailure())

		report.Cause.State = LogicFailure
		assert.True(t, report.IsCauseOfFailure())

		report.Cause.State = CommitFailure
		assert.True(t, report.IsCauseOfFailure())

		report.Cause.State = PostCommitFailure
		assert.True(t, report.IsCauseOfFailure())
	})

	t.Run("RecoveryForCommit", func(t *testing.T) {
		report := newTxnFailureReport("foo", "FooDataConn")

		assert.Equal(t, report.Cause.State, NoneByUncommitted)
		{
			assert.Equal(t, report.Rollback.State, NoneByNotRolledBack)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), RerunLogicAndCommit)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}

		report.Cause.State = NoneByCommitted
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForCommit(), NoActionRequired)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}

		report.Cause.State = LogicFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseThenRerunLogicAndCommit)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}

		report.Cause.State = CommitFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseThenRerunLogicAndCommit)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}

		report.Cause.State = PostCommitFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForCommit(), ResolveCauseThenRerunPostCommit)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}

		report.Cause.State = TxnFailureCauseState(9)
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForCommit(), InvestigateBecauseImpossible)
		}
	})

	t.Run("RecoveryForRollback", func(t *testing.T) {
		report := newTxnFailureReport("foo", "FooDataConn")

		assert.Equal(t, report.Cause.State, NoneByUncommitted)
		{
			assert.Equal(t, report.Rollback.State, NoneByNotRolledBack)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), NoActionRequired)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}

		report.Cause.State = NoneByCommitted
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForRollback(), ManualRollbackRequired)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}

		report.Cause.State = LogicFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), NoActionRequired)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}

		report.Cause.State = CommitFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), NoActionRequired)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), ResolveCauseAndInconsistency)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}

		report.Cause.State = PostCommitFailure
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForRollback(), ManualRollbackRequired)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}

		report.Cause.State = TxnFailureCauseState(9)
		{
			report.Rollback.State = NoneByNotRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = NoneByRolledBack
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = RollbackFailure
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)

			report.Rollback.State = TxnFailureRollbackState(99)
			assert.Equal(t, report.RecoveryForRollback(), InvestigateBecauseImpossible)
		}
	})
}
