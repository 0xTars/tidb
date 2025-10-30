// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncload

import (
	stderrors "errors"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// RetryCount is the max retry count for a sync load task.
// TODO(hawkingrei): There is no point in retrying sync load,
// because there will be other tasks requesting this task at the same time.
// As long as the subsequent tasks are normal, it will be fine. Too many retries
// will only cause congestion and delays
const RetryCount = 1

// MaxBatchSize is the maximum number of tasks to batch together for loading stats.
// Batching reduces storage round trips by loading multiple stat items in a single transaction.
const MaxBatchSize = 20

// BatchCollectionTimeout is the maximum time to wait for collecting tasks into a batch.
// A small timeout ensures low latency while still allowing batching under high load.
const BatchCollectionTimeout = 2 * time.Millisecond

// GetSyncLoadConcurrencyByCPU returns the concurrency of sync load by CPU.
func GetSyncLoadConcurrencyByCPU() int {
	core := runtime.GOMAXPROCS(0)
	if core <= 8 {
		return 5
	} else if core <= 16 {
		return 6
	} else if core <= 32 {
		return 8
	}
	return 10
}

// statsSyncLoad is used to load statistics synchronously when needed by SQL queries.
//
// It maintains two channels for handling statistics load tasks:
// - NeededItemsCh: High priority channel for tasks that haven't timed out yet (Higher priority)
// - TimeoutItemsCh: Lower priority channel for tasks that exceeded their timeout (Lower priority)
//
// The main workflow:
// 1. collect_column_stats_usage rule requests statistics via SendLoadRequests
// 2. Tasks are created and placed in channels
// 3. Worker goroutines pick up tasks from channels
// 4. Statistics are loaded from storage
// 5. Loaded statistics are cached via updateCachedItem for future use
// 6. Results are checked and stats are used in the SQL query
//
// It uses singleflight pattern to deduplicate concurrent requests for the same statistics.
// Requests that exceed their timeout are moved to a lower priority channel to be processed
// when there are no urgent requests.
type statsSyncLoad struct {
	statsHandle    statstypes.StatsHandle
	neededItemsCh  chan *statstypes.NeededItemTask
	timeoutItemsCh chan *statstypes.NeededItemTask
	// This mutex protects the statsCache from concurrent modifications by multiple workers.
	// Since multiple workers may update the statsCache for the same table simultaneously,
	// the mutex ensures thread-safety during these updates.
	mutexForStatsCache sync.Mutex
}

var globalStatsSyncLoadSingleFlight singleflight.Group

// NewStatsSyncLoad creates a new StatsSyncLoad.
func NewStatsSyncLoad(statsHandle statstypes.StatsHandle) statstypes.StatsSyncLoad {
	s := &statsSyncLoad{statsHandle: statsHandle}
	cfg := config.GetGlobalConfig()
	s.neededItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	s.timeoutItemsCh = make(chan *statstypes.NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	return s
}

type statsWrapper struct {
	colInfo *model.ColumnInfo
	idxInfo *model.IndexInfo
	col     *statistics.Column
	idx     *statistics.Index
}

// SendLoadRequests send neededColumns requests
func (s *statsSyncLoad) SendLoadRequests(sc *stmtctx.StatementContext, neededHistItems []model.StatsLoadItem, timeout time.Duration) error {
	remainedItems := s.removeHistLoadedColumns(neededHistItems)

	failpoint.Inject("assertSyncLoadItems", func(val failpoint.Value) {
		if sc.OptimizeTracer != nil {
			count := val.(int)
			if len(remainedItems) != count {
				panic("remained items count wrong")
			}
		}
	})
	if len(remainedItems) <= 0 {
		return nil
	}
	sc.StatsLoad.Timeout = timeout
	sc.StatsLoad.NeededItems = remainedItems
	sc.StatsLoad.ResultCh = make([]<-chan singleflight.Result, 0, len(remainedItems))
	for _, item := range remainedItems {
		localItem := item
		resultCh := globalStatsSyncLoadSingleFlight.DoChan(localItem.Key(), func() (any, error) {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			task := &statstypes.NeededItemTask{
				Item:      localItem,
				ToTimeout: time.Now().Local().Add(timeout),
				ResultCh:  make(chan stmtctx.StatsLoadResult, 1),
			}
			select {
			case s.neededItemsCh <- task:
				metrics.SyncLoadDedupCounter.Inc()
				select {
				case <-timer.C:
					return nil, errors.New("sync load took too long to return")
				case result, ok := <-task.ResultCh:
					intest.Assert(ok, "task.ResultCh cannot be closed")
					return result, nil
				}
			case <-timer.C:
				return nil, errors.New("sync load stats channel is full and timeout sending task to channel")
			}
		})
		sc.StatsLoad.ResultCh = append(sc.StatsLoad.ResultCh, resultCh)
	}
	sc.StatsLoad.LoadStartTime = time.Now()
	return nil
}

// SyncWaitStatsLoad sync waits loading of neededColumns and return false if timeout
func (*statsSyncLoad) SyncWaitStatsLoad(sc *stmtctx.StatementContext) error {
	if len(sc.StatsLoad.NeededItems) <= 0 {
		return nil
	}
	var errorMsgs []string
	defer func() {
		if len(errorMsgs) > 0 {
			statslogutil.StatsLogger().Warn("SyncWaitStatsLoad meets error",
				zap.Strings("errors", errorMsgs))
		}
		sc.StatsLoad.NeededItems = nil
	}()
	resultCheckMap := map[model.TableItemID]struct{}{}
	for _, col := range sc.StatsLoad.NeededItems {
		resultCheckMap[col.TableItemID] = struct{}{}
	}
	timer := time.NewTimer(sc.StatsLoad.Timeout)
	defer timer.Stop()
	for _, resultCh := range sc.StatsLoad.ResultCh {
		select {
		case result, ok := <-resultCh:
			metrics.SyncLoadCounter.Inc()
			if !ok {
				return errors.New("sync load stats channel closed unexpectedly")
			}
			// this error is from statsSyncLoad.SendLoadRequests which start to task and send task into worker,
			// not the stats loading error
			if result.Err != nil {
				errorMsgs = append(errorMsgs, result.Err.Error())
			} else {
				val := result.Val.(stmtctx.StatsLoadResult)
				// this error is from the stats loading error
				if val.HasError() {
					errorMsgs = append(errorMsgs, val.ErrorMsg())
				}
				delete(resultCheckMap, val.Item)
			}
		case <-timer.C:
			metrics.SyncLoadCounter.Inc()
			metrics.SyncLoadTimeoutCounter.Inc()
			return errors.New("sync load stats timeout")
		}
	}
	if len(resultCheckMap) == 0 {
		metrics.SyncLoadHistogram.Observe(float64(time.Since(sc.StatsLoad.LoadStartTime).Milliseconds()))
		return nil
	}
	return nil
}

// removeHistLoadedColumns removed having-hist columns based on neededColumns and statsCache.
func (s *statsSyncLoad) removeHistLoadedColumns(neededItems []model.StatsLoadItem) []model.StatsLoadItem {
	remainedItems := make([]model.StatsLoadItem, 0, len(neededItems))
	for _, item := range neededItems {
		tbl, ok := s.statsHandle.Get(item.TableID)
		if !ok {
			continue
		}
		if item.IsIndex {
			_, loadNeeded := tbl.IndexIsLoadNeeded(item.ID)
			if loadNeeded {
				remainedItems = append(remainedItems, item)
			}
			continue
		}
		_, loadNeeded, _ := tbl.ColumnIsLoadNeeded(item.ID, item.FullLoad)
		if loadNeeded {
			remainedItems = append(remainedItems, item)
		}
	}
	return remainedItems
}

// AppendNeededItem appends needed columns/indices to ch, it is only used for test
func (s *statsSyncLoad) AppendNeededItem(task *statstypes.NeededItemTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.neededItemsCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

var errExit = errors.New("Stop loading since domain is closed")

// SubLoadWorker loads hist data for each column.
// It uses batch processing to load multiple stat items in a single transaction for better performance.
func (s *statsSyncLoad) SubLoadWorker(exit chan struct{}, exitWg *util.WaitGroupEnhancedWrapper) {
	defer func() {
		exitWg.Done()
		statslogutil.StatsLogger().Info("SubLoadWorker: exited.")
	}()

	// Track failed tasks that need retry
	var failedTasks []*statstypes.NeededItemTask

	for {
		var tasks []*statstypes.NeededItemTask
		var err error

		// Process any failed tasks from previous iteration first
		if len(failedTasks) > 0 {
			tasks = failedTasks
			failedTasks = nil
		} else {
			// Drain batch of tasks from channels
			tasks, err = s.drainBatchTasks(exit)
			if err != nil {
				if err == errExit {
					statslogutil.StatsLogger().Info("SubLoadWorker: exits now because the domain is closed.")
					return
				}
				statslogutil.StatsErrVerboseSampleLogger().Warn("SubLoadWorker: failed to drain tasks", zap.Error(err))
				r := rand.Intn(500)
				time.Sleep(s.statsHandle.Lease()/10 + time.Duration(r)*time.Microsecond)
				continue
			}
		}

		if len(tasks) == 0 {
			continue
		}

		// Try batch processing first
		batchErr := s.HandleBatchTasks(tasks)
		if batchErr != nil {
			statslogutil.StatsLogger().Warn("SubLoadWorker: batch processing encountered error",
				zap.Int("batchSize", len(tasks)),
				zap.Error(batchErr))

			// Retry failed tasks individually on next iteration
			for _, task := range tasks {
				if isVaildForRetry(task) {
					failedTasks = append(failedTasks, task)
				} else {
					// Exceeded retry limit, send error result
					result := stmtctx.StatsLoadResult{
						Item:  task.Item.TableItemID,
						Error: batchErr,
					}
					task.ResultCh <- result
				}
			}

			if len(failedTasks) > 0 {
				// To avoid the thundering herd effect
				r := rand.Intn(500)
				time.Sleep(s.statsHandle.Lease()/10 + time.Duration(r)*time.Microsecond)
			}
		}
	}
}

// HandleOneTask handles last task if not nil, else handle a new task from chan, and return current task if fail somewhere.
//   - If the task is handled successfully, return nil, nil.
//   - If the task is timeout, return the task and nil. The caller should retry the timeout task without sleep.
//   - If the task is failed, return the task, error. The caller should retry the timeout task with sleep.
func (s *statsSyncLoad) HandleOneTask(lastTask *statstypes.NeededItemTask, exit chan struct{}) (task *statstypes.NeededItemTask, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("stats loading panicked", zap.Any("error", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	if lastTask == nil {
		task, err = s.drainColTask(exit)
		if err != nil {
			if err != errExit {
				statslogutil.StatsLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
			}
			return task, err
		}
	} else {
		task = lastTask
	}
	result := stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
	err = s.handleOneItemTask(task)
	if err == nil {
		task.ResultCh <- result
		return nil, nil
	}
	if !isVaildForRetry(task) {
		result.Error = err
		task.ResultCh <- result
		return nil, nil
	}
	return task, err
}

func isVaildForRetry(task *statstypes.NeededItemTask) bool {
	task.Retry++
	return task.Retry <= RetryCount
}

func (s *statsSyncLoad) handleOneItemTask(task *statstypes.NeededItemTask) (err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("handleOneItemTask panicked", zap.Any("recover", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()

	return s.statsHandle.SPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			sctx.GetSessionVars().StmtCtx.Priority = mysql.HighPriority
			defer func() {
				sctx.GetSessionVars().StmtCtx.Priority = mysql.NoPriority
			}()
			return s.handleOneItemTaskWithSCtx(sctx, task)
		})
	})
}

// handleOneItemTaskWithSCtx contains the core business logic for handling one item task.
// This method preserves git blame history by keeping the original logic intact.
func (s *statsSyncLoad) handleOneItemTaskWithSCtx(sctx sessionctx.Context, task *statstypes.NeededItemTask) error {
	var skipTypes map[string]struct{}
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to get global variable", zap.Error(err))
	} else {
		skipTypes = variable.ParseAnalyzeSkipColumnTypes(val)
	}

	item := task.Item.TableItemID
	statsTbl, ok := s.statsHandle.Get(item.TableID)

	if !ok {
		return nil
	}
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := s.statsHandle.TableInfoByID(is, item.TableID)
	if !ok {
		return nil
	}
	tblInfo := tbl.Meta()
	isPkIsHandle := tblInfo.PKIsHandle
	wrapper := &statsWrapper{}
	if item.IsIndex {
		index, loadNeeded := statsTbl.IndexIsLoadNeeded(item.ID)
		if !loadNeeded {
			return nil
		}
		if index != nil {
			wrapper.idxInfo = index.Info
		} else {
			wrapper.idxInfo = tblInfo.FindIndexByID(item.ID)
		}
	} else {
		col, loadNeeded, analyzed := statsTbl.ColumnIsLoadNeeded(item.ID, task.Item.FullLoad)
		if !loadNeeded {
			return nil
		}
		if col != nil {
			wrapper.colInfo = col.Info
		} else {
			// Now, we cannot init the column info in the ColAndIdxExistenceMap when to disable lite-init-stats.
			// so we have to get the column info from the domain.
			wrapper.colInfo = tblInfo.GetColumnByID(item.ID)
		}
		if skipTypes != nil {
			_, skip := skipTypes[types.TypeToStr(wrapper.colInfo.FieldType.GetType(), wrapper.colInfo.FieldType.GetCharset())]
			if skip {
				return nil
			}
		}

		// If this column is not analyzed yet and we don't have it in memory.
		// We create a fake one for the pseudo estimation.
		// Otherwise, it will trigger the sync/async load again, even if the column has not been analyzed.
		if !analyzed {
			wrapper.col = statistics.EmptyColumn(item.TableID, isPkIsHandle, wrapper.colInfo)
			s.updateCachedItem(item, wrapper.col, wrapper.idx, task.Item.FullLoad)
			return nil
		}
	}
	failpoint.Inject("handleOneItemTaskPanic", nil)
	t := time.Now()
	needUpdate := false
	wrapper, err = s.readStatsForOneItem(sctx, item, wrapper, isPkIsHandle, task.Item.FullLoad)
	if stderrors.Is(err, errGetHistMeta) {
		return nil
	}
	if err != nil {
		return err
	}
	if item.IsIndex {
		if wrapper.idxInfo != nil {
			needUpdate = true
		}
	} else {
		if wrapper.colInfo != nil {
			needUpdate = true
		}
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if needUpdate {
		s.updateCachedItem(item, wrapper.col, wrapper.idx, task.Item.FullLoad)
	}
	return nil
}

var errGetHistMeta = errors.New("fail to get hist meta")

// readStatsForOneItem reads hist for one column/index, TODO load data via kv-get asynchronously
func (*statsSyncLoad) readStatsForOneItem(sctx sessionctx.Context, item model.TableItemID, w *statsWrapper, isPkIsHandle bool, fullLoad bool) (*statsWrapper, error) {
	failpoint.Inject("mockReadStatsForOnePanic", nil)
	failpoint.Inject("mockReadStatsForOneFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail ReadStatsForOne error"))
		}
	})
	var hg *statistics.Histogram
	var err error
	isIndexFlag := int64(0)
	hg, statsVer, err := storage.HistMetaFromStorageWithHighPriority(sctx, &item, w.colInfo)
	if err != nil {
		return nil, err
	}
	if hg == nil {
		statslogutil.StatsSampleLogger().Warn(
			"Histogram not found, possibly due to DDL event is not handled, please consider analyze the table",
			zap.Int64("tableID", item.TableID),
			zap.Int64("histID", item.ID),
			zap.Bool("isIndex", item.IsIndex),
		)
		return nil, errGetHistMeta
	}
	if item.IsIndex {
		isIndexFlag = 1
	}
	var cms *statistics.CMSketch
	var topN *statistics.TopN
	if fullLoad {
		if item.IsIndex {
			hg, err = storage.HistogramFromStorageWithPriority(sctx, item.TableID, item.ID, types.NewFieldType(mysql.TypeBlob), hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation, kv.PriorityHigh)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			hg, err = storage.HistogramFromStorageWithPriority(sctx, item.TableID, item.ID, &w.colInfo.FieldType, hg.NDV, int(isIndexFlag), hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation, kv.PriorityHigh)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		cms, topN, err = storage.CMSketchAndTopNFromStorageWithHighPriority(sctx, item.TableID, isIndexFlag, item.ID, statsVer)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if item.IsIndex {
		idxHist := &statistics.Index{
			Histogram:  *hg,
			CMSketch:   cms,
			TopN:       topN,
			Info:       w.idxInfo,
			StatsVer:   statsVer,
			PhysicalID: item.TableID,
		}
		if statsVer != statistics.Version0 {
			if fullLoad {
				idxHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			} else {
				idxHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
		w.idx = idxHist
	} else {
		colHist := &statistics.Column{
			PhysicalID: item.TableID,
			Histogram:  *hg,
			Info:       w.colInfo,
			CMSketch:   cms,
			TopN:       topN,
			IsHandle:   isPkIsHandle && mysql.HasPriKeyFlag(w.colInfo.GetFlag()),
			StatsVer:   statsVer,
		}
		if colHist.StatsAvailable() {
			if fullLoad {
				colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			} else {
				colHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
		w.col = colHist
	}
	return w, nil
}

// drainColTask will hang until a task can return, and either task or error will be returned.
// The task will be drained from NeededItemsCh first, if no task, then TimeoutItemsCh.
func (s *statsSyncLoad) drainColTask(exit chan struct{}) (*statstypes.NeededItemTask, error) {
	// select NeededItemsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-s.neededItemsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededItemsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				s.writeToTimeoutChan(s.timeoutItemsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-s.timeoutItemsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-s.neededItemsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededItemsCh, maybe the chan is closed")
				}
				// send task back to TimeoutItemsCh and return the task drained from NeededItemsCh
				s.writeToTimeoutChan(s.timeoutItemsCh, task)
				return task0, nil
			default:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from TimeoutItemsCh, maybe the chan is closed")
				}
				// NeededItemsCh is empty now, handle task from TimeoutItemsCh
				return task, nil
			}
		}
	}
}

// writeToTimeoutChan writes in a nonblocking way, and if the channel queue is full, it's ok to drop the task.
func (*statsSyncLoad) writeToTimeoutChan(taskCh chan *statstypes.NeededItemTask, task *statstypes.NeededItemTask) {
	select {
	case taskCh <- task:
	default:
	}
}

// tableBatch represents a batch of tasks for a single table.
type tableBatch struct {
	tableID int64
	tasks   []*statstypes.NeededItemTask
}

// drainBatchTasks collects multiple tasks from the channels to enable batch processing.
// It will collect up to MaxBatchSize tasks or wait up to BatchCollectionTimeout, whichever comes first.
// Returns a slice of tasks grouped by table ID for efficient batch loading.
func (s *statsSyncLoad) drainBatchTasks(exit chan struct{}) ([]*statstypes.NeededItemTask, error) {
	batch := make([]*statstypes.NeededItemTask, 0, MaxBatchSize)
	timer := time.NewTimer(BatchCollectionTimeout)
	defer timer.Stop()

	// Try to get the first task (blocking)
	firstTask, err := s.drainColTask(exit)
	if err != nil {
		return nil, err
	}
	batch = append(batch, firstTask)

	// Try to collect more tasks without blocking (up to MaxBatchSize)
	for len(batch) < MaxBatchSize {
		select {
		case <-exit:
			return batch, nil
		case <-timer.C:
			// Timeout reached, return current batch
			return batch, nil
		case task, ok := <-s.neededItemsCh:
			if !ok {
				return batch, nil
			}
			// Check if task has already timed out
			if time.Now().After(task.ToTimeout) {
				s.writeToTimeoutChan(s.timeoutItemsCh, task)
				continue
			}
			batch = append(batch, task)
		case task, ok := <-s.timeoutItemsCh:
			// Only take from timeout channel if needed channel is empty
			select {
			case task0, ok0 := <-s.neededItemsCh:
				if ok0 {
					if time.Now().After(task0.ToTimeout) {
						s.writeToTimeoutChan(s.timeoutItemsCh, task0)
					} else {
						batch = append(batch, task0)
					}
				}
				if ok {
					s.writeToTimeoutChan(s.timeoutItemsCh, task)
				}
			default:
				if !ok {
					return batch, nil
				}
				batch = append(batch, task)
			}
		default:
			// No more tasks available without blocking, return current batch
			return batch, nil
		}
	}
	return batch, nil
}

// groupTasksByTable groups tasks by table ID for batch processing.
// Tasks for the same table can be loaded together in a single transaction.
func groupTasksByTable(tasks []*statstypes.NeededItemTask) []tableBatch {
	tableMap := make(map[int64][]*statstypes.NeededItemTask)
	for _, task := range tasks {
		tableID := task.Item.TableID
		tableMap[tableID] = append(tableMap[tableID], task)
	}

	batches := make([]tableBatch, 0, len(tableMap))
	for tableID, tableTasks := range tableMap {
		batches = append(batches, tableBatch{
			tableID: tableID,
			tasks:   tableTasks,
		})
	}
	return batches
}

// handleBatchTasksForTable handles a batch of tasks for a single table using batch storage loading.
func (s *statsSyncLoad) handleBatchTasksForTable(sctx sessionctx.Context, batch tableBatch) error {
	var skipTypes map[string]struct{}
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to get global variable", zap.Error(err))
	} else {
		skipTypes = variable.ParseAnalyzeSkipColumnTypes(val)
	}

	statsTbl, ok := s.statsHandle.Get(batch.tableID)
	if !ok {
		// Table doesn't exist or was dropped
		for _, task := range batch.tasks {
			task.ResultCh <- stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
		}
		return nil
	}

	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := s.statsHandle.TableInfoByID(is, batch.tableID)
	if !ok {
		// Table info not found
		for _, task := range batch.tasks {
			task.ResultCh <- stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
		}
		return nil
	}
	tblInfo := tbl.Meta()
	isPkIsHandle := tblInfo.PKIsHandle

	// Prepare column info map and items list for batch loading
	colInfoMap := make(map[int64]*model.ColumnInfo)
	items := make([]model.TableItemID, 0, len(batch.tasks))
	taskMap := make(map[model.TableItemID]*statstypes.NeededItemTask)

	for _, task := range batch.tasks {
		item := task.Item.TableItemID

		// Check if already loaded
		if item.IsIndex {
			_, loadNeeded := statsTbl.IndexIsLoadNeeded(item.ID)
			if !loadNeeded {
				task.ResultCh <- stmtctx.StatsLoadResult{Item: item}
				continue
			}
		} else {
			_, loadNeeded, analyzed := statsTbl.ColumnIsLoadNeeded(item.ID, task.Item.FullLoad)
			if !loadNeeded {
				task.ResultCh <- stmtctx.StatsLoadResult{Item: item}
				continue
			}

			// If not analyzed, create empty column
			if !analyzed {
				colInfo := tblInfo.GetColumnByID(item.ID)
				if colInfo != nil {
					wrapper := &statsWrapper{col: statistics.EmptyColumn(item.TableID, isPkIsHandle, colInfo)}
					s.updateCachedItem(item, wrapper.col, nil, task.Item.FullLoad)
				}
				task.ResultCh <- stmtctx.StatsLoadResult{Item: item}
				continue
			}

			// Get column info
			colInfo := tblInfo.GetColumnByID(item.ID)
			if colInfo == nil {
				task.ResultCh <- stmtctx.StatsLoadResult{Item: item, Error: errors.Errorf("column not found: %d", item.ID)}
				continue
			}

			// Check if column should be skipped
			if skipTypes != nil {
				_, skip := skipTypes[types.TypeToStr(colInfo.FieldType.GetType(), colInfo.FieldType.GetCharset())]
				if skip {
					task.ResultCh <- stmtctx.StatsLoadResult{Item: item}
					continue
				}
			}

			colInfoMap[item.ID] = colInfo
		}

		// Add to items for batch loading
		items = append(items, item)
		taskMap[item] = task
	}

	// If no items need loading, return
	if len(items) == 0 {
		return nil
	}

	// Batch load stats for all items
	batchResults, err := storage.BatchLoadStatsForTable(sctx, batch.tableID, items, true, colInfoMap, isPkIsHandle)
	if err != nil {
		// On batch error, fall back to individual loading for retry
		return err
	}

	// Process results and update cache
	for _, result := range batchResults {
		task := taskMap[result.Item]
		if task == nil {
			continue
		}

		loadResult := stmtctx.StatsLoadResult{Item: result.Item}

		if result.Err != nil {
			loadResult.Error = result.Err
			task.ResultCh <- loadResult
			continue
		}

		// Build the statistics object
		var wrapper *statsWrapper
		if result.Item.IsIndex {
			idxInfo := tblInfo.FindIndexByID(result.Item.ID)
			if idxInfo == nil {
				loadResult.Error = errors.Errorf("index info not found for index_id=%d", result.Item.ID)
				task.ResultCh <- loadResult
				continue
			}

			wrapper = &statsWrapper{
				idxInfo: idxInfo,
				idx: &statistics.Index{
					Histogram:         *result.Hg,
					CMSketch:          result.Cms,
					TopN:              result.TopN,
					Info:              idxInfo,
					StatsVer:          result.StatsVer,
					PhysicalID:        batch.tableID,
					StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
				},
			}
		} else {
			colInfo := colInfoMap[result.Item.ID]
			wrapper = &statsWrapper{
				colInfo: colInfo,
				col: &statistics.Column{
					PhysicalID:        batch.tableID,
					Histogram:         *result.Hg,
					Info:              colInfo,
					CMSketch:          result.Cms,
					TopN:              result.TopN,
					IsHandle:          isPkIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
					StatsVer:          result.StatsVer,
					StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
				},
			}
		}

		// Update cache
		s.updateCachedItem(result.Item, wrapper.col, wrapper.idx, task.Item.FullLoad)

		// Send result
		task.ResultCh <- loadResult
	}

	return nil
}

// HandleBatchTasks handles multiple tasks as a batch, grouped by table for efficiency.
func (s *statsSyncLoad) HandleBatchTasks(tasks []*statstypes.NeededItemTask) error {
	if len(tasks) == 0 {
		return nil
	}

	// Group tasks by table
	batches := groupTasksByTable(tasks)

	// Process each table's batch
	for _, batch := range batches {
		err := s.statsHandle.SPool().WithSession(func(se *syssession.Session) error {
			return se.WithSessionContext(func(sctx sessionctx.Context) error {
				sctx.GetSessionVars().StmtCtx.Priority = mysql.HighPriority
				defer func() {
					sctx.GetSessionVars().StmtCtx.Priority = mysql.NoPriority
				}()
				return s.handleBatchTasksForTable(sctx, batch)
			})
		})

		if err != nil {
			// On error, fall back to individual processing for this batch
			statslogutil.StatsLogger().Warn("batch processing failed, falling back to individual processing",
				zap.Int64("tableID", batch.tableID),
				zap.Int("batchSize", len(batch.tasks)),
				zap.Error(err))

			// Process each task individually as fallback
			for _, task := range batch.tasks {
				if taskErr := s.handleOneItemTask(task); taskErr != nil {
					result := stmtctx.StatsLoadResult{Item: task.Item.TableItemID, Error: taskErr}
					task.ResultCh <- result
				} else {
					task.ResultCh <- stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
				}
			}
		}
	}

	return nil
}

// updateCachedItem updates the column/index hist to global statsCache.
func (s *statsSyncLoad) updateCachedItem(item model.TableItemID, colHist *statistics.Column, idxHist *statistics.Index, fullLoaded bool) (updated bool) {
	s.mutexForStatsCache.Lock()
	defer s.mutexForStatsCache.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok := s.statsHandle.Get(item.TableID)
	if !ok {
		return false
	}
	if !item.IsIndex && colHist != nil {
		c := tbl.GetCol(item.ID)
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if c != nil && (c.IsFullLoad() || !fullLoaded) {
			return false
		}
		tbl = tbl.CopyAs(statistics.ColumnMapWritable)
		tbl.SetCol(item.ID, colHist)

		// If the column is analyzed we refresh the map for the possible change.
		if colHist.StatsAvailable() {
			tbl.ColAndIdxExistenceMap.InsertCol(item.ID, true)
		}
		// All the objects shares the same stats version. Update it here.
		if colHist.StatsVer != statistics.Version0 {
			tbl.StatsVer = statistics.Version0
		}
		// we have to refresh the map for the possible change to ensure that the map information is not missing.
		tbl.ColAndIdxExistenceMap.InsertCol(item.ID, colHist.StatsAvailable())
	} else if item.IsIndex && idxHist != nil {
		index := tbl.GetIdx(item.ID)
		// - If the stats is fully loaded,
		// - If the stats is meta-loaded and we also just need the meta.
		if index != nil && (index.IsFullLoad() || !fullLoaded) {
			return true
		}
		tbl = tbl.CopyAs(statistics.IndexMapWritable)
		tbl.SetIdx(item.ID, idxHist)
		// If the index is analyzed we refresh the map for the possible change.
		if idxHist.IsAnalyzed() {
			tbl.ColAndIdxExistenceMap.InsertIndex(item.ID, true)
			// All the objects shares the same stats version. Update it here.
			tbl.StatsVer = statistics.Version0
		}
	}
	s.statsHandle.UpdateStatsCache(statstypes.CacheUpdate{
		Updated: []*statistics.Table{tbl},
	})
	return true
}
