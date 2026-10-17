package sabi

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/sttk/errs"
)

type Fail int

const (
	Fail_None = iota
	Fail_Setup
	Fail_PreCommit
	Fail_Commit
	Fail_PostCommit
	Fail_Rollback
	Fail_PreCommitBecomeCommitted
	Fail_CreateDataConn
)

type Logger struct {
	mutex sync.Mutex
	logs  list.List
}

func NewLogger() *Logger {
	return &Logger{}
}

func (lg *Logger) PushBack(logs ...string) {
	lg.mutex.Lock()
	defer lg.mutex.Unlock()

	for _, s := range logs {
		lg.logs.PushBack(s)
	}
}

func (lg *Logger) Logs() []string {
	logs := make([]string, 0, lg.logs.Len())
	iter := lg.logs.Front()
	for {
		if iter == nil {
			break
		}
		logs = append(logs, iter.Value.(string))
		iter = iter.Next()
	}
	return logs
}

type SyncDataConn struct {
	id        int8
	committed bool
	fail      Fail
	logger    *Logger
}

func NewSyncDataConn(id int8, logger *Logger, fail Fail) SyncDataConn {
	return SyncDataConn{
		id:        id,
		committed: false,
		fail:      fail,
		logger:    logger,
	}
}

func (conn *SyncDataConn) Commit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_Commit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Commit %d failed", conn.id))
		return errs.New("ZZZ")
	}
	conn.committed = true
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Commit %d", conn.id))
	return errs.Ok()
}

func (conn *SyncDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_PreCommit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PreCommit %d failed", conn.id))
		return errs.New("zzz")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PreCommit %d", conn.id))
	if conn.fail == Fail_PreCommitBecomeCommitted {
		conn.committed = true
	}
	return errs.Ok()
}

func (conn *SyncDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_PostCommit {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PostCommit %d failed", conn.id))
		return errs.New("!!!")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#PostCommit %d", conn.id))
	return errs.Ok()
}

func (conn *SyncDataConn) IsCommitted() bool {
	return conn.committed
}

func (conn *SyncDataConn) Rollback(ag *AsyncGroup) errs.Err {
	if conn.fail == Fail_Rollback {
		conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Rollback %d failed", conn.id))
		return errs.New("???")
	}
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Rollback %d", conn.id))
	return errs.Ok()
}

func (conn *SyncDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	conn.logger.PushBack(
		fmt.Sprintf("SyncDataConn#OnTxnFailure %d", conn.id),
		fmt.Sprintf("TxnFailureReport=%+v", reports),
	)
}

func (conn *SyncDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("SyncDataConn#Close %d", conn.id))
}

type AsyncDataConn struct {
	id        int8
	committed bool
	fail      Fail
	logger    *Logger
}

func NewAsyncDataConn(id int8, logger *Logger, fail Fail) AsyncDataConn {
	return AsyncDataConn{
		id:        id,
		committed: false,
		fail:      fail,
		logger:    logger,
	}
}

func (conn *AsyncDataConn) Commit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		if conn.fail == Fail_Commit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Commit %d failed", conn.id))
			return errs.New("YYY")
		}
		conn.committed = true
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Commit %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}

func (conn *AsyncDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		if conn.fail == Fail_PreCommit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PreCommit %d failed", conn.id))
			return errs.New("yyy")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PreCommit %d", conn.id))
		if conn.fail == Fail_PreCommitBecomeCommitted {
			conn.committed = true
		}
		return errs.Ok()
	})
	return errs.Ok()
}

func (conn *AsyncDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		if conn.fail == Fail_PostCommit {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PostCommit %d failed", conn.id))
			return errs.New("!!!")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#PostCommit %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}

func (conn *AsyncDataConn) IsCommitted() bool {
	return conn.committed
}

func (conn *AsyncDataConn) Rollback(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		if conn.fail == Fail_Rollback {
			conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Rollback %d failed", conn.id))
			return errs.New("???")
		}
		conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Rollback %d", conn.id))
		return errs.Ok()
	})
	return errs.Ok()
}

func (conn *AsyncDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	ag.Add(func() errs.Err {
		time.Sleep(100)
		conn.logger.PushBack(
			fmt.Sprintf("AsyncDataConn#OnTxnFailure %d", conn.id),
			fmt.Sprintf("TxnFailureReport=%+v", reports),
		)
		return errs.Ok()
	})
}

func (conn *AsyncDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("AsyncDataConn#Close %d", conn.id))
}

type NoCommitDataConn struct {
	id     int8
	fail   Fail
	logger *Logger
}

func NewNoCommitDataConn(id int8, logger *Logger, fail Fail) NoCommitDataConn {
	return NoCommitDataConn{
		id:     id,
		fail:   fail,
		logger: logger,
	}
}

func (conn *NoCommitDataConn) Commit(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#Commit %d", conn.id))
	return errs.Ok()
}

func (conn *NoCommitDataConn) PreCommit(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#PreCommit %d", conn.id))
	return errs.Ok()
}

func (conn *NoCommitDataConn) PostCommit(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#PostCommit %d", conn.id))
	return errs.Ok()
}

func (conn *NoCommitDataConn) IsCommitted() bool {
	return false
}

func (conn *NoCommitDataConn) Rollback(ag *AsyncGroup) errs.Err {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#Rollback %d", conn.id))
	return errs.Ok()
}

func (conn *NoCommitDataConn) OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport) {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#OnTxnFailure %d", conn.id))
	conn.logger.PushBack(fmt.Sprintf("TxnFailureReport=%+v", reports))
}

func (conn *NoCommitDataConn) Close() {
	conn.logger.PushBack(fmt.Sprintf("NoCommitDataConn#Close %d", conn.id))
}

type SyncDataSrc struct {
	id     int8
	fail   Fail
	logger *Logger
}

func NewSyncDataSrc(id int8, logger *Logger, fail Fail) SyncDataSrc {
	logger.PushBack(fmt.Sprintf("SyncDataSrc#New %d", id))
	return SyncDataSrc{id: id, logger: logger, fail: fail}
}

func (ds *SyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	if ds.fail == Fail_Setup {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc#Setup %d failed", ds.id))
		return errs.New("XXX")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc#Setup %d", ds.id))
	return errs.Ok()
}

func (ds *SyncDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc#Close %d", ds.id))
}

func (ds *SyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == Fail_CreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("SyncDataSrc#CreateDataConn %d failed", ds.id))
		return nil, errs.New("eeee")
	}
	ds.logger.PushBack(fmt.Sprintf("SyncDataSrc#CreateDataConn %d", ds.id))
	return &SyncDataConn{}, errs.Ok()
}

type AsyncDataSrc struct {
	id     int8
	fail   Fail
	logger *Logger
	wait   time.Duration
}

func NewAsyncDataSrc(id int8, logger *Logger, fail Fail) AsyncDataSrc {
	logger.PushBack(fmt.Sprintf("AsyncDataSrc#New %d", id))
	return AsyncDataSrc{id: id, logger: logger, fail: fail}
}

func (ds *AsyncDataSrc) Setup(ag *AsyncGroup) errs.Err {
	ag.Add(func() errs.Err {
		time.Sleep(ds.wait)
		if ds.fail == Fail_Setup {
			ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc#Setup %d failed", ds.id))
			return errs.New("XXX")
		}
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc#Setup %d", ds.id))
		return errs.Ok()
	})
	return errs.Ok()
}

func (ds *AsyncDataSrc) Close() {
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc#Close %d", ds.id))
}

func (ds *AsyncDataSrc) CreateDataConn() (DataConn, errs.Err) {
	if ds.fail == Fail_CreateDataConn {
		ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc#CreateDataConn %d failed", ds.id))
		return nil, errs.New("eeee")
	}
	ds.logger.PushBack(fmt.Sprintf("AsyncDataSrc#CreateDataConn %d", ds.id))
	return &AsyncDataConn{}, errs.Ok()
}
