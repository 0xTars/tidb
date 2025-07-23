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

package storage_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestUpdateStatsMetaVersionForGC(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(),
		ast.NewCIStr("test"), ast.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(context.Background(), is)
	require.NoError(t, err)

	// Reset one partition stats.
	p0 := pi.Definitions[0]
	err = h.UpdateStatsMetaVersionForGC(p0.ID)
	require.NoError(t, err)

	// Get partition stats from stats_meta table.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?",
		p0.ID,
	).Rows()
	require.Equal(t, 1, len(rows))
	// Parse version from stats_meta.
	version := rows[0][0].(string)

	// Check stats_meta_history again. The version should be the same.
	rows = testKit.MustQuery(
		"select count(*) from mysql.stats_meta_history where table_id = ? and version = ?",
		p0.ID,
		version,
	).Rows()
	require.Equal(t, 1, len(rows))
}

func TestSlowStatsSaving(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving", "return(true)")
	require.NoError(t, err)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving")
		require.NoError(t, err)
	}()
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t with 0 topn")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(),
		ast.NewCIStr("test"), ast.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	// Get stats version from mysql.stats_meta.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?",
		tableInfo.ID,
	).Rows()
	require.Equal(t, 1, len(rows))
	version := rows[0][0].(string)
	versionUint64, err := strconv.ParseUint(version, 10, 64)
	require.NoError(t, err)
	// Get stats version from mysql.stats_histograms.
	rows = testKit.MustQuery(
		"select version from mysql.stats_histograms where table_id = ?",
		tableInfo.ID,
	).Rows()
	require.Equal(t, 2, len(rows))
	histVersion := rows[0][0].(string)
	histVersionUint64, err := strconv.ParseUint(histVersion, 10, 64)
	require.NoError(t, err)
	require.True(t, versionUint64 > histVersionUint64, "The version in stats_meta should be greater than stats_histograms.")
}

func TestSlowStatsSavingForPartitionedTable(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving", "return(true)")
	require.NoError(t, err)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving")
		require.NoError(t, err)
	}()
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t with 0 topn")
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(),
		ast.NewCIStr("test"), ast.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	// Note: We deliberately focus on checking the global stats version here.
	// For global stats, `SaveColOrIdxStatsToStorage` is used to persist statistics to storage.
	// We primarily verify the global stats version to confirm successful saving after the slow stats saving process.
	// Get stats version from mysql.stats_meta.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?",
		tableInfo.ID,
	).Rows()
	require.Equal(t, 1, len(rows))
	version := rows[0][0].(string)
	versionUint64, err := strconv.ParseUint(version, 10, 64)
	require.NoError(t, err)
	// Get stats version from mysql.stats_histograms.
	rows = testKit.MustQuery(
		"select version from mysql.stats_histograms where table_id = ?",
		tableInfo.ID,
	).Rows()
	require.Equal(t, 3, len(rows))
	for _, row := range rows {
		histVersion := row[0].(string)
		histVersionUint64, err := strconv.ParseUint(histVersion, 10, 64)
		require.NoError(t, err)
		require.True(t, versionUint64 > histVersionUint64, "The version in stats_meta should be greater than stats_histograms.")
	}
}

func TestFailedToHandleSlowStatsSaving(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving", "return(true)")
	require.NoError(t, err)
	err = failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/storage/failToSaveStats", "return(true)")
	require.NoError(t, err)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/storage/slowStatsSaving")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/storage/failToSaveStats")
		require.NoError(t, err)
	}()
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustGetErrMsg("analyze table t with 0 topn", "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later")
}

func TestDumpStatsDeltasAndAnalyzeTableConcurrently(t *testing.T) {
	// Enable the failpoint to introduce latency between locking operations
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/pointGetLockLatency", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/pointGetLockLatency"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	testKit.MustExec("create table t1 (a int, b int, index idx(a))")
	testKit.MustExec("create table t2 (a int, b int, index idx(a))")
	testKit.MustExec("create table t3 (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("insert into t1 values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("insert into t2 values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("insert into t3 values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t all columns")
	testKit.MustExec("analyze table t1 all columns")
	testKit.MustExec("analyze table t2 all columns")
	testKit.MustExec("analyze table t3 all columns")

	// Use a channel to signal when analyze operations are complete
	analyzeDone := make(chan struct{})

	// Use a counter to track completed analyze operations
	var analyzeCounter int32
	var analyzeMutex sync.Mutex

	// Start goroutines to dump stats deltas and analyze table concurrently.
	wg := sync.WaitGroup{}
	wg.Add(5) // 1 for stats dumping + 4 for analyze operations

	go func() {
		defer wg.Done()
		// Create a separate testKit for this goroutine
		goroutineTestKit := testkit.NewTestKit(t, store)
		goroutineTestKit.MustExec("use test")
		for {
			select {
			case <-analyzeDone:
				// Analyze operations are complete, stop inserting and dumping
				return
			default:
				goroutineTestKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
				goroutineTestKit.MustExec("insert into t1 values (1,2),(2,2),(6,2),(11,2),(16,2)")
				goroutineTestKit.MustExec("insert into t2 values (1,2),(2,2),(6,2),(11,2),(16,2)")
				goroutineTestKit.MustExec("insert into t3 values (1,2),(2,2),(6,2),(11,2),(16,2)")
				require.NoError(t, h.DumpStatsDeltaToKV(true))
			}
		}
	}()
	// Create dedicated goroutines for each table analyze operation
	for _, tableName := range []string{"t", "t1", "t2", "t3"} {
		go func(table string) {
			defer wg.Done()
			// Create a separate testKit for this goroutine
			goroutineTestKit := testkit.NewTestKit(t, store)
			goroutineTestKit.MustExec("use test")
			for i := 0; i < 50; i++ {
				goroutineTestKit.MustExec("analyze table " + table + " all columns")
			}
			// Increment counter when this table's analyze operations are complete
			analyzeMutex.Lock()
			analyzeCounter++
			if analyzeCounter == 4 { // All 4 tables completed
				close(analyzeDone)
			}
			analyzeMutex.Unlock()
		}(tableName)
	}
	wg.Wait()
}
