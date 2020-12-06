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

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
)

// WaitForSubordinateStart waits until the deadline for replication to start.
// This validates the current main is correct and can be connected to.
func WaitForSubordinateStart(mysqld MysqlDaemon, subordinateStartDeadline int) error {
	var rowMap map[string]string
	for subordinateWait := 0; subordinateWait < subordinateStartDeadline; subordinateWait++ {
		status, err := mysqld.SubordinateStatus()
		if err != nil {
			return err
		}

		if status.SubordinateRunning() {
			return nil
		}
		time.Sleep(time.Second)
	}

	errorKeys := []string{"Last_Error", "Last_IO_Error", "Last_SQL_Error"}
	errs := make([]string, 0, len(errorKeys))
	for _, key := range errorKeys {
		if rowMap[key] != "" {
			errs = append(errs, key+": "+rowMap[key])
		}
	}
	if len(errs) != 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

// StartSubordinate starts a subordinate.
func (mysqld *Mysqld) StartSubordinate(hookExtraEnv map[string]string) error {
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.StartSubordinateCommand()}); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_subordinate")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// StartSubordinateUntilAfter starts a subordinate until replication has come to `targetPos`, then it stops replication
func (mysqld *Mysqld) StartSubordinateUntilAfter(ctx context.Context, targetPos mysql.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.StartSubordinateUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StopSubordinate stops a subordinate.
func (mysqld *Mysqld) StopSubordinate(hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_subordinate")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.StopSubordinateCommand()})
}

// GetMysqlPort returns mysql port
func (mysqld *Mysqld) GetMysqlPort() (int32, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW VARIABLES LIKE 'port'")
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no port variable in mysql")
	}
	utemp, err := sqltypes.ToUint64(qr.Rows[0][1])
	if err != nil {
		return 0, err
	}
	return int32(utemp), nil
}

// IsReadOnly return true if the instance is read only
func (mysqld *Mysqld) IsReadOnly() (bool, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(qr.Rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// SetReadOnly set/unset the read_only flag
func (mysqld *Mysqld) SetReadOnly(on bool) error {
	query := "SET GLOBAL read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(context.TODO(), query)
}

var (
	// ErrNotMain means there is no main status
	ErrNotMain = errors.New("no main status")
)

// SetSuperReadOnly set/unset the super_read_only flag
func (mysqld *Mysqld) SetSuperReadOnly(on bool) error {
	query := "SET GLOBAL super_read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(context.TODO(), query)
}

// WaitMainPos lets subordinates wait to given replication position
func (mysqld *Mysqld) WaitMainPos(ctx context.Context, targetPos mysql.Position) error {
	// Get a connection.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// Find the query to run, run it.
	query, err := conn.WaitUntilPositionCommand(ctx, targetPos)
	if err != nil {
		return err
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("WaitUntilPositionCommand(%v) failed: %v", query, err)
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result format from WaitUntilPositionCommand(%v): %#v", query, qr)
	}
	result := qr.Rows[0][0]
	if result.IsNull() {
		return fmt.Errorf("WaitUntilPositionCommand(%v) failed: replication is probably stopped", query)
	}
	if result.ToString() == "-1" {
		return fmt.Errorf("timed out waiting for position %v", targetPos)
	}
	return nil
}

// SubordinateStatus returns the subordinate replication statuses
func (mysqld *Mysqld) SubordinateStatus() (mysql.SubordinateStatus, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return mysql.SubordinateStatus{}, err
	}
	defer conn.Recycle()

	return conn.ShowSubordinateStatus()
}

// MainPosition returns the main replication position.
func (mysqld *Mysqld) MainPosition() (mysql.Position, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Recycle()

	return conn.MainPosition()
}

// SetSubordinatePosition sets the replication position at which the subordinate will resume
// when its replication is started.
func (mysqld *Mysqld) SetSubordinatePosition(ctx context.Context, pos mysql.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.SetSubordinatePositionCommands(pos)
	log.Infof("Executing commands to set subordinate position: %v", cmds)
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// SetMain makes the provided host / port the main. It optionally
// stops replication before, and starts it after.
func (mysqld *Mysqld) SetMain(ctx context.Context, mainHost string, mainPort int, subordinateStopBefore bool, subordinateStartAfter bool) error {
	params, err := dbconfigs.WithCredentials(mysqld.dbcfgs.Repl())
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := []string{}
	if subordinateStopBefore {
		cmds = append(cmds, conn.StopSubordinateCommand())
	}
	smc := conn.SetMainCommand(params, mainHost, mainPort, int(mainConnectRetry.Seconds()))
	cmds = append(cmds, smc)
	if subordinateStartAfter {
		cmds = append(cmds, conn.StartSubordinateCommand())
	}
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplication resets all replication for this host.
func (mysqld *Mysqld) ResetReplication(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.ResetReplicationCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
// | Id   | User    | Host                | db   | Command     | Time | State                                                          | Info             |
// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
// | 9792 | vt_repl | host:port           | NULL | Binlog Dump |   54 | Has sent all binlog to subordinate; waiting for binlog to be updated | NULL             |
// | 9797 | vt_dba  | localhost           | NULL | Query       |    0 | NULL                                                           | show processlist |
// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
//
// Array indices for the results of SHOW PROCESSLIST.
const (
	colConnectionID = iota
	colUsername
	colClientAddr
	colDbName
	colCommand
)

const (
	// this is the command used by mysql subordinates
	binlogDumpCommand = "Binlog Dump"
)

// FindSubordinates gets IP addresses for all currently connected subordinates.
func FindSubordinates(mysqld MysqlDaemon) ([]string, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		// Check for prefix, since it could be "Binlog Dump GTID".
		if strings.HasPrefix(row[colCommand].ToString(), binlogDumpCommand) {
			host := row[colClientAddr].ToString()
			if host == "localhost" {
				// If we have a local binlog streamer, it will
				// show up as being connected
				// from 'localhost' through the local
				// socket. Ignore it.
				continue
			}
			host, _, err = netutil.SplitHostPort(host)
			if err != nil {
				return nil, fmt.Errorf("FindSubordinates: malformed addr %v", err)
			}
			var ips []string
			ips, err = net.LookupHost(host)
			if err != nil {
				return nil, fmt.Errorf("FindSubordinates: LookupHost failed %v", err)
			}
			addrs = append(addrs, ips...)
		}
	}

	return addrs, nil
}

// EnableBinlogPlayback prepares the server to play back events from a binlog stream.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) EnableBinlogPlayback() error {
	// Get a connection.
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// See if we have a command to run, and run it.
	cmd := conn.EnableBinlogPlaybackCommand()
	if cmd == "" {
		return nil
	}
	if err := mysqld.ExecuteSuperQuery(context.TODO(), cmd); err != nil {
		log.Errorf("EnableBinlogPlayback: cannot run query '%v': %v", cmd, err)
		return fmt.Errorf("EnableBinlogPlayback: cannot run query '%v': %v", cmd, err)
	}

	log.Info("EnableBinlogPlayback: successfully ran %v", cmd)
	return nil
}

// DisableBinlogPlayback returns the server to the normal state after streaming.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) DisableBinlogPlayback() error {
	// Get a connection.
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// See if we have a command to run, and run it.
	cmd := conn.DisableBinlogPlaybackCommand()
	if cmd == "" {
		return nil
	}
	if err := mysqld.ExecuteSuperQuery(context.TODO(), cmd); err != nil {
		log.Errorf("DisableBinlogPlayback: cannot run query '%v': %v", cmd, err)
		return fmt.Errorf("DisableBinlogPlayback: cannot run query '%v': %v", cmd, err)
	}

	log.Info("DisableBinlogPlayback: successfully ran '%v'", cmd)
	return nil
}

// SetSemiSyncEnabled enables or disables semi-sync replication for
// main and/or subordinate mode.
func (mysqld *Mysqld) SetSemiSyncEnabled(main, subordinate bool) error {
	log.Infof("Setting semi-sync mode: main=%v, subordinate=%v", main, subordinate)

	// Convert bool to int.
	var m, s int
	if main {
		m = 1
	}
	if subordinate {
		s = 1
	}

	err := mysqld.ExecuteSuperQuery(context.TODO(), fmt.Sprintf(
		"SET GLOBAL rpl_semi_sync_main_enabled = %v, GLOBAL rpl_semi_sync_subordinate_enabled = %v",
		m, s))
	if err != nil {
		return fmt.Errorf("can't set semi-sync mode: %v; make sure plugins are loaded in my.cnf", err)
	}
	return nil
}

// SemiSyncEnabled returns whether semi-sync is enabled for main or subordinate.
// If the semi-sync plugin is not loaded, we assume semi-sync is disabled.
func (mysqld *Mysqld) SemiSyncEnabled() (main, subordinate bool) {
	vars, err := mysqld.fetchVariables(context.TODO(), "rpl_semi_sync_%_enabled")
	if err != nil {
		return false, false
	}
	main = (vars["rpl_semi_sync_main_enabled"] == "ON")
	subordinate = (vars["rpl_semi_sync_subordinate_enabled"] == "ON")
	return main, subordinate
}

// SemiSyncSubordinateStatus returns whether semi-sync is currently used by replication.
func (mysqld *Mysqld) SemiSyncSubordinateStatus() (bool, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW STATUS LIKE 'rpl_semi_sync_subordinate_status'")
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, errors.New("no rpl_semi_sync_subordinate_status variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}
