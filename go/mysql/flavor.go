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

package mysql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// ErrNotSubordinate means there is no subordinate status.
	// Returned by ShowSubordinateStatus().
	ErrNotSubordinate = errors.New("no subordinate status")
)

const (
	// mariaDBReplicationHackPrefix is the prefix of a version for MariaDB 10.0
	// versions, to work around replication bugs.
	mariaDBReplicationHackPrefix = "5.5.5-"
	// mariaDBVersionString is present in
	mariaDBVersionString = "MariaDB"
)

// flavor is the abstract interface for a flavor.
// Flavors are auto-detected upon connection using the server version.
// We have two major implementations (the main difference is the GTID
// handling):
// 1. Oracle MySQL 5.6, 5.7, 8.0, ...
// 2. MariaDB 10.X
type flavor interface {
	// mainGTIDSet returns the current GTIDSet of a server.
	mainGTIDSet(c *Conn) (GTIDSet, error)

	// startSubordinate returns the command to start the subordinate.
	startSubordinateCommand() string

	// startSubordinateUntilAfter will restart replication, but only allow it
	// to run until `pos` is reached. After reaching pos, replication will be stopped again
	startSubordinateUntilAfter(pos Position) string

	// stopSubordinate returns the command to stop the subordinate.
	stopSubordinateCommand() string

	// sendBinlogDumpCommand sends the packet required to start
	// dumping binlogs from the specified location.
	sendBinlogDumpCommand(c *Conn, subordinateID uint32, startPos Position) error

	// readBinlogEvent reads the next BinlogEvent from the connection.
	readBinlogEvent(c *Conn) (BinlogEvent, error)

	// resetReplicationCommands returns the commands to completely reset
	// replication on the host.
	resetReplicationCommands() []string

	// setSubordinatePositionCommands returns the commands to set the
	// replication position at which the subordinate will resume.
	setSubordinatePositionCommands(pos Position) []string

	// changeMainArg returns the specific parameter to add to
	// a change main command.
	changeMainArg() string

	// status returns the result of 'SHOW SLAVE STATUS',
	// with parsed replication position.
	status(c *Conn) (SubordinateStatus, error)

	// waitUntilPositionCommand returns the SQL command to issue
	// to wait until the given position, until the context
	// expires.  The command returns -1 if it times out. It
	// returns NULL if GTIDs are not enabled.
	waitUntilPositionCommand(ctx context.Context, pos Position) (string, error)

	// enableBinlogPlaybackCommand and disableBinlogPlaybackCommand return an
	// optional command to run to enable or disable binlog
	// playback. This is used internally in Google, as the
	// timestamp cannot be set by regular clients.
	enableBinlogPlaybackCommand() string
	disableBinlogPlaybackCommand() string
}

// fillFlavor fills in c.Flavor based on c.ServerVersion.
// This is the same logic as the ConnectorJ java client. We try to recognize
// MariaDB as much as we can, but default to MySQL.
//
// MariaDB note: the server version returned here might look like:
// 5.5.5-10.0.21-MariaDB-...
// If that is the case, we are removing the 5.5.5- prefix.
// Note on such servers, 'select version()' would return 10.0.21-MariaDB-...
// as well (not matching what c.ServerVersion is, but matching after we remove
// the prefix).
func (c *Conn) fillFlavor() {
	if strings.HasPrefix(c.ServerVersion, mariaDBReplicationHackPrefix) {
		c.ServerVersion = c.ServerVersion[len(mariaDBReplicationHackPrefix):]
		c.flavor = mariadbFlavor{}
		return
	}

	if strings.Contains(c.ServerVersion, mariaDBVersionString) {
		c.flavor = mariadbFlavor{}
		return
	}

	c.flavor = mysqlFlavor{}
}

//
// The following methods are dependent on the flavor.
// Only valid for client connections (will panic for server connections).
//

// IsMariaDB returns true iff the other side of the client connection
// is identified as MariaDB. Most applications should not care, but
// this is useful in tests.
func (c *Conn) IsMariaDB() bool {
	_, ok := c.flavor.(mariadbFlavor)
	return ok
}

// MainPosition returns the current main replication position.
func (c *Conn) MainPosition() (Position, error) {
	gtidSet, err := c.flavor.mainGTIDSet(c)
	if err != nil {
		return Position{}, err
	}
	return Position{
		GTIDSet: gtidSet,
	}, nil
}

// StartSubordinateCommand returns the command to start the subordinate.
func (c *Conn) StartSubordinateCommand() string {
	return c.flavor.startSubordinateCommand()
}

// StartSubordinateUntilAfterCommand returns the command to start the subordinate.
func (c *Conn) StartSubordinateUntilAfterCommand(pos Position) string {
	return c.flavor.startSubordinateUntilAfter(pos)
}

// StopSubordinateCommand returns the command to stop the subordinate.
func (c *Conn) StopSubordinateCommand() string {
	return c.flavor.stopSubordinateCommand()
}

// SendBinlogDumpCommand sends the flavor-specific version of
// the COM_BINLOG_DUMP command to start dumping raw binlog
// events over a subordinate connection, starting at a given GTID.
func (c *Conn) SendBinlogDumpCommand(subordinateID uint32, startPos Position) error {
	return c.flavor.sendBinlogDumpCommand(c, subordinateID, startPos)
}

// ReadBinlogEvent reads the next BinlogEvent. This must be used
// in conjunction with SendBinlogDumpCommand.
func (c *Conn) ReadBinlogEvent() (BinlogEvent, error) {
	return c.flavor.readBinlogEvent(c)
}

// ResetReplicationCommands returns the commands to completely reset
// replication on the host.
func (c *Conn) ResetReplicationCommands() []string {
	return c.flavor.resetReplicationCommands()
}

// SetSubordinatePositionCommands returns the commands to set the
// replication position at which the subordinate will resume
// when it is later reparented with SetMainCommands.
func (c *Conn) SetSubordinatePositionCommands(pos Position) []string {
	return c.flavor.setSubordinatePositionCommands(pos)
}

// SetMainCommand returns the command to use the provided main
// as the new main (without changing any GTID position).
// It is guaranteed to be called with replication stopped.
// It should not start or stop replication.
func (c *Conn) SetMainCommand(params *ConnParams, mainHost string, mainPort int, mainConnectRetry int) string {
	args := []string{
		fmt.Sprintf("MASTER_HOST = '%s'", mainHost),
		fmt.Sprintf("MASTER_PORT = %d", mainPort),
		fmt.Sprintf("MASTER_USER = '%s'", params.Uname),
		fmt.Sprintf("MASTER_PASSWORD = '%s'", params.Pass),
		fmt.Sprintf("MASTER_CONNECT_RETRY = %d", mainConnectRetry),
	}
	if params.SslEnabled() {
		args = append(args, "MASTER_SSL = 1")
	}
	if params.SslCa != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CA = '%s'", params.SslCa))
	}
	if params.SslCaPath != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CAPATH = '%s'", params.SslCaPath))
	}
	if params.SslCert != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CERT = '%s'", params.SslCert))
	}
	if params.SslKey != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_KEY = '%s'", params.SslKey))
	}
	args = append(args, c.flavor.changeMainArg())
	return "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")
}

// resultToMap is a helper function used by ShowSubordinateStatus.
func resultToMap(qr *sqltypes.Result) (map[string]string, error) {
	if len(qr.Rows) == 0 {
		// The query succeeded, but there is no data.
		return nil, nil
	}
	if len(qr.Rows) > 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "query returned %d rows, expected 1", len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "query returned %d column names, expected %d", len(qr.Fields), len(qr.Rows[0]))
	}

	result := make(map[string]string, len(qr.Fields))
	for i, field := range qr.Fields {
		result[field.Name] = qr.Rows[0][i].ToString()
	}
	return result, nil
}

// parseSubordinateStatus parses the common fields of SHOW SLAVE STATUS.
func parseSubordinateStatus(fields map[string]string) SubordinateStatus {
	status := SubordinateStatus{
		MainHost:      fields["Main_Host"],
		SubordinateIORunning:  fields["Subordinate_IO_Running"] == "Yes",
		SubordinateSQLRunning: fields["Subordinate_SQL_Running"] == "Yes",
	}
	parseInt, _ := strconv.ParseInt(fields["Main_Port"], 10, 0)
	status.MainPort = int(parseInt)
	parseInt, _ = strconv.ParseInt(fields["Connect_Retry"], 10, 0)
	status.MainConnectRetry = int(parseInt)
	parseUint, _ := strconv.ParseUint(fields["Seconds_Behind_Main"], 10, 0)
	status.SecondsBehindMain = uint(parseUint)
	return status
}

// ShowSubordinateStatus executes the right SHOW SLAVE STATUS command,
// and returns a parse Position with other fields.
func (c *Conn) ShowSubordinateStatus() (SubordinateStatus, error) {
	return c.flavor.status(c)
}

// WaitUntilPositionCommand returns the SQL command to issue
// to wait until the given position, until the context
// expires.  The command returns -1 if it times out. It
// returns NULL if GTIDs are not enabled.
func (c *Conn) WaitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	return c.flavor.waitUntilPositionCommand(ctx, pos)
}

// EnableBinlogPlaybackCommand returns a command to run to enable
// binlog playback.
func (c *Conn) EnableBinlogPlaybackCommand() string {
	return c.flavor.enableBinlogPlaybackCommand()
}

// DisableBinlogPlaybackCommand returns a command to run to disable
// binlog playback.
func (c *Conn) DisableBinlogPlaybackCommand() string {
	return c.flavor.disableBinlogPlaybackCommand()
}
