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

package mysqlctl

import (
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// MysqlDaemon is the interface we use for abstracting Mysqld.
type MysqlDaemon interface {
	// methods related to mysql running or not
	Start(ctx context.Context, cnf *Mycnf, mysqldArgs ...string) error
	Shutdown(ctx context.Context, cnf *Mycnf, waitForMysqld bool) error
	RunMysqlUpgrade() error
	ReinitConfig(ctx context.Context, cnf *Mycnf) error
	Wait(ctx context.Context, cnf *Mycnf) error

	// GetMysqlPort returns the current port mysql is listening on.
	GetMysqlPort() (int32, error)

	// replication related methods
	StartSubordinate(hookExtraEnv map[string]string) error
	StartSubordinateUntilAfter(ctx context.Context, pos mysql.Position) error
	StopSubordinate(hookExtraEnv map[string]string) error
	SubordinateStatus() (mysql.SubordinateStatus, error)
	SetSemiSyncEnabled(main, subordinate bool) error
	SemiSyncEnabled() (main, subordinate bool)
	SemiSyncSubordinateStatus() (bool, error)

	// reparenting related methods
	ResetReplication(ctx context.Context) error
	MainPosition() (mysql.Position, error)
	IsReadOnly() (bool, error)
	SetReadOnly(on bool) error
	SetSuperReadOnly(on bool) error
	SetSubordinatePosition(ctx context.Context, pos mysql.Position) error
	SetMain(ctx context.Context, mainHost string, mainPort int, subordinateStopBefore bool, subordinateStartAfter bool) error
	WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error

	// Deprecated: use MainPosition() instead
	DemoteMain() (mysql.Position, error)

	WaitMainPos(context.Context, mysql.Position) error

	// PromoteSubordinate makes the subordinate the new main. It will not change
	// the read_only state of the server.
	PromoteSubordinate(map[string]string) (mysql.Position, error)

	// Schema related methods
	GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error)
	GetColumns(dbName, table string) ([]string, error)
	GetPrimaryKeyColumns(dbName, table string) ([]string, error)
	PreflightSchemaChange(dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)
	ApplySchemaChange(dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	// GetAppConnection returns a app connection to be able to talk to the database.
	GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error)
	// GetDbaConnection returns a dba connection.
	GetDbaConnection() (*dbconnpool.DBConnection, error)
	// GetAllPrivsConnection returns an allprivs connection (for user with all privileges except SUPER).
	GetAllPrivsConnection() (*dbconnpool.DBConnection, error)

	// ExecuteSuperQueryList executes a list of queries, no result
	ExecuteSuperQueryList(ctx context.Context, queryList []string) error

	// FetchSuperQuery executes one query, returns the result
	FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error)

	// EnableBinlogPlayback enables playback of binlog events
	EnableBinlogPlayback() error

	// DisableBinlogPlayback disable playback of binlog events
	DisableBinlogPlayback() error

	// Close will close this instance of Mysqld. It will wait for all dba
	// queries to be finished.
	Close()
}
