/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fakemysqldaemon

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	// db is the fake SQL DB we may use for some queries.
	db *fakesqldb.DB

	// appPool is set if db is set.
	appPool *dbconnpool.ConnectionPool

	// Running is used by Start / Shutdown
	Running bool

	// MysqlPort will be returned by GetMysqlPort(). Set to -1 to
	// return an error.
	MysqlPort int32

	// Replicating is updated when calling StartSubordinate / StopSubordinate
	// (it is not used at all when calling SubordinateStatus, it is the
	// test owner responsibility to have these two match)
	Replicating bool

	// CurrentMainPosition is returned by MainPosition
	// and SubordinateStatus
	CurrentMainPosition mysql.Position

	// SubordinateStatusError is used by SubordinateStatus
	SubordinateStatusError error

	// CurrentMainHost is returned by SubordinateStatus
	CurrentMainHost string

	// CurrentMainport is returned by SubordinateStatus
	CurrentMainPort int

	// SecondsBehindMain is returned by SubordinateStatus
	SecondsBehindMain uint

	// ReadOnly is the current value of the flag
	ReadOnly bool

	// SuperReadOnly is the current value of the flag
	SuperReadOnly bool

	// SetSubordinatePositionPos is matched against the input of SetSubordinatePosition.
	// If it doesn't match, SetSubordinatePosition will return an error.
	SetSubordinatePositionPos mysql.Position

	// StartSubordinateUntilAfterPos is matched against the input
	StartSubordinateUntilAfterPos mysql.Position

	// SetMainInput is matched against the input of SetMain
	// (as "%v:%v"). If it doesn't match, SetMain will return an error.
	SetMainInput string

	// WaitMainPosition is checked by WaitMainPos, if the
	// same it returns nil, if different it returns an error
	WaitMainPosition mysql.Position

	// PromoteSubordinateResult is returned by PromoteSubordinate
	PromoteSubordinateResult mysql.Position

	// SchemaFunc provides the return value for GetSchema.
	// If not defined, the "Schema" field will be used instead, see below.
	SchemaFunc func() (*tabletmanagerdatapb.SchemaDefinition, error)

	// Schema will be returned by GetSchema. If nil we'll
	// return an error.
	Schema *tabletmanagerdatapb.SchemaDefinition

	// PreflightSchemaChangeResult will be returned by PreflightSchemaChange.
	// If nil we'll return an error.
	PreflightSchemaChangeResult []*tabletmanagerdatapb.SchemaChangeResult

	// ApplySchemaChangeResult will be returned by ApplySchemaChange.
	// If nil we'll return an error.
	ApplySchemaChangeResult *tabletmanagerdatapb.SchemaChangeResult

	// ExpectedExecuteSuperQueryList is what we expect
	// ExecuteSuperQueryList to be called with. If it doesn't
	// match, ExecuteSuperQueryList will return an error.
	// Note each string is just a substring if it begins with SUB,
	// so we support partial queries (useful when queries contain
	// data fields like timestamps)
	ExpectedExecuteSuperQueryList []string

	// ExpectedExecuteSuperQueryCurrent is the current index of the queries
	// we expect
	ExpectedExecuteSuperQueryCurrent int

	// FetchSuperQueryResults is used by FetchSuperQuery
	FetchSuperQueryMap map[string]*sqltypes.Result

	// BinlogPlayerEnabled is used by {Enable,Disable}BinlogPlayer
	BinlogPlayerEnabled sync2.AtomicBool

	// SemiSyncMainEnabled represents the state of rpl_semi_sync_main_enabled.
	SemiSyncMainEnabled bool
	// SemiSyncSubordinateEnabled represents the state of rpl_semi_sync_subordinate_enabled.
	SemiSyncSubordinateEnabled bool

	// TimeoutHook is a func that can be called at the beginning of any method to fake a timeout.
	// all a test needs to do is make it { return context.DeadlineExceeded }
	TimeoutHook func() error
}

// NewFakeMysqlDaemon returns a FakeMysqlDaemon where mysqld appears
// to be running, based on a fakesqldb.DB.
// 'db' can be nil if the test doesn't use a database at all.
func NewFakeMysqlDaemon(db *fakesqldb.DB) *FakeMysqlDaemon {
	result := &FakeMysqlDaemon{
		db:      db,
		Running: true,
	}
	if db != nil {
		result.appPool = dbconnpool.NewConnectionPool("AppConnPool", 5, time.Minute, 0)
		result.appPool.Open(db.ConnParams(), stats.NewTimings("", "", ""))
	}
	return result
}

// Start is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Start(ctx context.Context, cnf *mysqlctl.Mycnf, mysqldArgs ...string) error {
	if fmd.Running {
		return fmt.Errorf("fake mysql daemon already running")
	}
	fmd.Running = true
	return nil
}

// Shutdown is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Shutdown(ctx context.Context, cnf *mysqlctl.Mycnf, waitForMysqld bool) error {
	if !fmd.Running {
		return fmt.Errorf("fake mysql daemon not running")
	}
	fmd.Running = false
	return nil
}

// RunMysqlUpgrade is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RunMysqlUpgrade() error {
	return nil
}

// ReinitConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ReinitConfig(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// RefreshConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RefreshConfig(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// Wait is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) Wait(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// GetMysqlPort is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetMysqlPort() (int32, error) {
	if fmd.MysqlPort == -1 {
		return 0, fmt.Errorf("FakeMysqlDaemon.GetMysqlPort returns an error")
	}
	return fmd.MysqlPort, nil
}

// SubordinateStatus is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SubordinateStatus() (mysql.SubordinateStatus, error) {
	if fmd.SubordinateStatusError != nil {
		return mysql.SubordinateStatus{}, fmd.SubordinateStatusError
	}
	return mysql.SubordinateStatus{
		Position:            fmd.CurrentMainPosition,
		SecondsBehindMain: fmd.SecondsBehindMain,
		SubordinateIORunning:      fmd.Replicating,
		SubordinateSQLRunning:     fmd.Replicating,
		MainHost:          fmd.CurrentMainHost,
		MainPort:          fmd.CurrentMainPort,
	}, nil
}

// ResetReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) ResetReplication(ctx context.Context) error {
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE RESET ALL REPLICATION",
	})
}

// MainPosition is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) MainPosition() (mysql.Position, error) {
	return fmd.CurrentMainPosition, nil
}

// IsReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) IsReadOnly() (bool, error) {
	return fmd.ReadOnly, nil
}

// SetReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetReadOnly(on bool) error {
	fmd.ReadOnly = on
	return nil
}

// SetSuperReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetSuperReadOnly(on bool) error {
	fmd.SuperReadOnly = on
	fmd.ReadOnly = on
	return nil
}

// StartSubordinate is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StartSubordinate(hookExtraEnv map[string]string) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"START SLAVE",
	})
}

// StartSubordinateUntilAfter is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StartSubordinateUntilAfter(ctx context.Context, pos mysql.Position) error {
	if !reflect.DeepEqual(fmd.StartSubordinateUntilAfterPos, pos) {
		return fmt.Errorf("wrong pos for StartSubordinateUntilAfter: expected %v got %v", fmd.SetSubordinatePositionPos, pos)
	}

	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"START SLAVE UNTIL AFTER",
	})
}

// StopSubordinate is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StopSubordinate(hookExtraEnv map[string]string) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"STOP SLAVE",
	})
}

// SetSubordinatePosition is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetSubordinatePosition(ctx context.Context, pos mysql.Position) error {
	if !reflect.DeepEqual(fmd.SetSubordinatePositionPos, pos) {
		return fmt.Errorf("wrong pos for SetSubordinatePosition: expected %v got %v", fmd.SetSubordinatePositionPos, pos)
	}
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE SET SLAVE POSITION",
	})
}

// SetMain is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetMain(ctx context.Context, mainHost string, mainPort int, subordinateStopBefore bool, subordinateStartAfter bool) error {
	input := fmt.Sprintf("%v:%v", mainHost, mainPort)
	if fmd.SetMainInput != input {
		return fmt.Errorf("wrong input for SetMainCommands: expected %v got %v", fmd.SetMainInput, input)
	}
	cmds := []string{}
	if subordinateStopBefore {
		cmds = append(cmds, "STOP SLAVE")
	}
	cmds = append(cmds, "FAKE SET MASTER")
	if subordinateStartAfter {
		cmds = append(cmds, "START SLAVE")
	}
	return fmd.ExecuteSuperQueryList(ctx, cmds)
}

// WaitForReparentJournal is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	return nil
}

// Deprecated: use mysqld.MainPosition() instead
func (fmd *FakeMysqlDaemon) DemoteMain() (mysql.Position, error) {
	return fmd.CurrentMainPosition, nil
}

// WaitMainPos is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitMainPos(_ context.Context, pos mysql.Position) error {
	if fmd.TimeoutHook != nil {
		return fmd.TimeoutHook()
	}
	if reflect.DeepEqual(fmd.WaitMainPosition, pos) {
		return nil
	}
	return fmt.Errorf("wrong input for WaitMainPos: expected %v got %v", fmd.WaitMainPosition, pos)
}

// PromoteSubordinate is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PromoteSubordinate(hookExtraEnv map[string]string) (mysql.Position, error) {
	return fmd.PromoteSubordinateResult, nil
}

// ExecuteSuperQueryList is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ExecuteSuperQueryList(ctx context.Context, queryList []string) error {
	for _, query := range queryList {
		// test we still have a query to compare
		if fmd.ExpectedExecuteSuperQueryCurrent >= len(fmd.ExpectedExecuteSuperQueryList) {
			return fmt.Errorf("unexpected extra query in ExecuteSuperQueryList: %v", query)
		}

		// compare the query
		expected := fmd.ExpectedExecuteSuperQueryList[fmd.ExpectedExecuteSuperQueryCurrent]
		fmd.ExpectedExecuteSuperQueryCurrent++
		if strings.HasPrefix(expected, "SUB") {
			// remove the SUB from the expected,
			// and truncate the query to length(expected)
			expected = expected[3:]
			if len(query) > len(expected) {
				query = query[:len(expected)]
			}
		}
		if expected != query {
			return fmt.Errorf("wrong query for ExecuteSuperQueryList: expected %v got %v", expected, query)
		}

		// intercept some queries to update our status
		switch query {
		case "START SLAVE":
			fmd.Replicating = true
		case "STOP SLAVE":
			fmd.Replicating = false
		}
	}
	return nil
}

// FetchSuperQuery returns the results from the map, if any
func (fmd *FakeMysqlDaemon) FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	if fmd.FetchSuperQueryMap == nil {
		return nil, fmt.Errorf("unexpected query: %v", query)
	}

	qr, ok := fmd.FetchSuperQueryMap[query]
	if !ok {
		return nil, fmt.Errorf("unexpected query: %v", query)
	}
	return qr, nil
}

// EnableBinlogPlayback is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) EnableBinlogPlayback() error {
	fmd.BinlogPlayerEnabled.Set(true)
	return nil
}

// DisableBinlogPlayback disable playback of binlog events
func (fmd *FakeMysqlDaemon) DisableBinlogPlayback() error {
	fmd.BinlogPlayerEnabled.Set(false)
	return nil
}

// Close is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Close() {
	if fmd.appPool != nil {
		fmd.appPool.Close()
	}
}

// CheckSuperQueryList returns an error if all the queries we expected
// haven't been seen.
func (fmd *FakeMysqlDaemon) CheckSuperQueryList() error {
	if fmd.ExpectedExecuteSuperQueryCurrent != len(fmd.ExpectedExecuteSuperQueryList) {
		return fmt.Errorf("SuperQueryList wasn't consumed, saw %v queries, was expecting %v", fmd.ExpectedExecuteSuperQueryCurrent, len(fmd.ExpectedExecuteSuperQueryList))
	}
	return nil
}

// GetSchema is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fmd.SchemaFunc != nil {
		return fmd.SchemaFunc()
	}
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return tmutils.FilterTables(fmd.Schema, tables, excludeTables, includeViews)
}

// GetColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetColumns(dbName, table string) ([]string, error) {
	return []string{}, nil
}

// GetPrimaryKeyColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetPrimaryKeyColumns(dbName, table string) ([]string, error) {
	return []string{}, nil
}

// PreflightSchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PreflightSchemaChange(dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fmd.PreflightSchemaChangeResult == nil {
		return nil, fmt.Errorf("no preflight result defined")
	}
	return fmd.PreflightSchemaChangeResult, nil
}

// ApplySchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ApplySchemaChange(dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fmd.ApplySchemaChangeResult == nil {
		return nil, fmt.Errorf("no apply schema defined")
	}
	return fmd.ApplySchemaChangeResult, nil
}

// GetAppConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error) {
	return fmd.appPool.Get(ctx)
}

// GetDbaConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetDbaConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(fmd.db.ConnParams(), stats.NewTimings("", "", ""))
}

// GetAllPrivsConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAllPrivsConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(fmd.db.ConnParams(), stats.NewTimings("", "", ""))
}

// SetSemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetSemiSyncEnabled(main, subordinate bool) error {
	fmd.SemiSyncMainEnabled = main
	fmd.SemiSyncSubordinateEnabled = subordinate
	return nil
}

// SemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncEnabled() (main, subordinate bool) {
	return fmd.SemiSyncMainEnabled, fmd.SemiSyncSubordinateEnabled
}

// SemiSyncSubordinateStatus is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncSubordinateStatus() (bool, error) {
	// The fake assumes the status worked.
	return fmd.SemiSyncSubordinateEnabled, nil
}
