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
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// mysqlFlavor implements the Flavor interface for Mysql.
type mysqlFlavor struct{}

// mainGTIDSet is part of the Flavor interface.
func (mysqlFlavor) mainGTIDSet(c *Conn) (GTIDSet, error) {
	qr, err := c.ExecuteFetch("SELECT @@GLOBAL.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for gtid_executed: %#v", qr)
	}
	return parseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

func (mysqlFlavor) startSubordinateCommand() string {
	return "START SLAVE"
}

func (mysqlFlavor) startSubordinateUntilAfter(pos Position) string {
	return fmt.Sprintf("START SLAVE UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (mysqlFlavor) stopSubordinateCommand() string {
	return "STOP SLAVE"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mysqlFlavor) sendBinlogDumpCommand(c *Conn, subordinateID uint32, startPos Position) error {
	gtidSet, ok := startPos.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return c.WriteComBinlogDumpGTID(subordinateID, "", 4, 0, sidBlock)
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget main host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
		"SET GLOBAL rpl_semi_sync_main_enabled = false, GLOBAL rpl_semi_sync_subordinate_enabled = false", // semi-sync will be enabled if needed when subordinate is started.
	}
}

// setSubordinatePositionCommands is part of the Flavor interface.
func (mysqlFlavor) setSubordinatePositionCommands(pos Position) []string {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// setSubordinatePositionCommands is part of the Flavor interface.
func (mysqlFlavor) changeMainArg() string {
	return "MASTER_AUTO_POSITION = 1"
}

// status is part of the Flavor interface.
func (mysqlFlavor) status(c *Conn) (SubordinateStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return SubordinateStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a subordinate.
		return SubordinateStatus{}, ErrNotSubordinate
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return SubordinateStatus{}, err
	}

	status := parseSubordinateStatus(resultMap)
	status.Position.GTIDSet, err = parseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return SubordinateStatus{}, vterrors.Wrapf(err, "SubordinateStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}
	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.
func (mysqlFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	// A timeout of 0 means wait indefinitely.
	timeoutSeconds := 0
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return "", vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
		}

		// Only whole numbers of seconds are supported.
		timeoutSeconds = int(timeout.Seconds())
		if timeoutSeconds == 0 {
			// We don't want a timeout <1.0s to truncate down to become infinite.
			timeoutSeconds = 1
		}
	}

	return fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s', %v)", pos, timeoutSeconds), nil
}

// readBinlogEvent is part of the Flavor interface.
func (mysqlFlavor) readBinlogEvent(c *Conn) (BinlogEvent, error) {
	result, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch result[0] {
	case EOFPacket:
		return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", io.EOF)
	case ErrPacket:
		return nil, ParseErrorPacket(result)
	}
	return NewMysql56BinlogEvent(result[1:]), nil
}

// enableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) enableBinlogPlaybackCommand() string {
	return ""
}

// disableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) disableBinlogPlaybackCommand() string {
	return ""
}
