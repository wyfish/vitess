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

package testlib

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestInitMainShard is the good scenario test, where everything
// works as planned
func TestInitMainShard(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	db.AddQuery("CREATE DATABASE IF NOT EXISTS `vt_test_keyspace`", &sqltypes.Result{})

	// Create a main, a couple good subordinates
	main := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	goodSubordinate1 := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	goodSubordinate2 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, db)

	// Main: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	main.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	main.FakeMysqlDaemon.ReadOnly = true
	main.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)

	// Subordinate1: expect to be reset and re-parented
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.SetSubordinatePositionPos = main.FakeMysqlDaemon.CurrentMainPosition
	goodSubordinate1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	goodSubordinate1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate1.StartActionLoop(t, wr)
	defer goodSubordinate1.StopActionLoop(t)

	// Subordinate2: expect to be re-parented
	goodSubordinate2.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate2.FakeMysqlDaemon.SetSubordinatePositionPos = main.FakeMysqlDaemon.CurrentMainPosition
	goodSubordinate2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	goodSubordinate2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate2.StartActionLoop(t, wr)
	defer goodSubordinate2.StopActionLoop(t)

	// run InitShardMain
	if err := vp.Run([]string{"InitShardMain", "-wait_subordinate_timeout", "10s", main.Tablet.Keyspace + "/" + main.Tablet.Shard, topoproto.TabletAliasString(main.Tablet.Alias)}); err != nil {
		t.Fatalf("InitShardMain failed: %v", err)
	}

	// check what was run
	if main.FakeMysqlDaemon.ReadOnly {
		t.Errorf("main was not turned read-write")
	}
	si, err := ts.GetShard(ctx, main.Tablet.Keyspace, main.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if !topoproto.TabletAliasEqual(si.MainAlias, main.Tablet.Alias) {
		t.Errorf("unexpected shard main alias, got %v expected %v", si.MainAlias, main.Tablet.Alias)
	}
	if err := main.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("main.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	checkSemiSyncEnabled(t, true, true, main)
	checkSemiSyncEnabled(t, false, true, goodSubordinate1, goodSubordinate2)
}

// TestInitMainShardChecks makes sure the safety checks work
func TestInitMainShardChecks(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	main := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)

	// InitShardMain with an unknown tablet
	if err := wr.InitShardMain(ctx, main.Tablet.Keyspace, main.Tablet.Shard, &topodatapb.TabletAlias{
		Cell: main.Tablet.Alias.Cell,
		Uid:  main.Tablet.Alias.Uid + 1,
	}, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not in the shard") {
		t.Errorf("InitShardMain with unknown alias returned wrong error: %v", err)
	}

	// InitShardMain with two mains in the shard, no force flag
	// (main2 needs to run InitTablet with -force, as it is the second
	// main in the same shard)
	main2 := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, db, ForceInitTablet())
	if err := wr.InitShardMain(ctx, main2.Tablet.Keyspace, main2.Tablet.Shard, main2.Tablet.Alias, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not the only main in the shard") {
		t.Errorf("InitShardMain with two mains returned wrong error: %v", err)
	}

	// InitShardMain where the new main fails (use force flag
	// as we have 2 mains). We force the failure by making the
	// SQL commands executed on the main unexpected by the test fixture
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)
	main2.StartActionLoop(t, wr)
	defer main2.StopActionLoop(t)
	if err := wr.InitShardMain(ctx, main.Tablet.Keyspace, main.Tablet.Shard, main.Tablet.Alias, true /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "unexpected extra query") {
		t.Errorf("InitShardMain with new main failing in new main InitMain returned wrong error: %v", err)
	}
}

// TestInitMainShardOneSubordinateFails makes sure that if one subordinate fails to
// proceed, the action completes anyway
func TestInitMainShardOneSubordinateFails(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.New(t)
	defer db.Close()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a main, a couple subordinates
	main := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db)
	goodSubordinate := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, db)
	badSubordinate := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, db)

	// Main: set a plausible ReplicationPosition to return,
	// and expect to add entry in _vt.reparent_journal
	main.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	main.FakeMysqlDaemon.ReadOnly = true
	main.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)

	// goodSubordinate: expect to be re-parented
	goodSubordinate.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate.FakeMysqlDaemon.SetSubordinatePositionPos = main.FakeMysqlDaemon.CurrentMainPosition
	goodSubordinate.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	goodSubordinate.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate.StartActionLoop(t, wr)
	defer goodSubordinate.StopActionLoop(t)

	// badSubordinate: insert an error by failing the main hostname input
	// on purpose
	badSubordinate.FakeMysqlDaemon.ReadOnly = true
	badSubordinate.FakeMysqlDaemon.SetSubordinatePositionPos = main.FakeMysqlDaemon.CurrentMainPosition
	badSubordinate.FakeMysqlDaemon.SetMainInput = fmt.Sprintf("%v:%v", "", topoproto.MysqlPort(main.Tablet))
	badSubordinate.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
	}
	badSubordinate.StartActionLoop(t, wr)
	defer badSubordinate.StopActionLoop(t)

	// also change the main alias in the Shard object, to make sure it
	// is set back.
	_, err := ts.UpdateShardFields(ctx, main.Tablet.Keyspace, main.Tablet.Shard, func(si *topo.ShardInfo) error {
		// note it's OK to retry this and increment multiple times,
		// we just want it to be different
		si.MainAlias.Uid++
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// run InitShardMain without force, it fails because main is
	// changing.
	if err := wr.InitShardMain(ctx, main.Tablet.Keyspace, main.Tablet.Shard, main.Tablet.Alias, false /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is not the shard main") {
		t.Errorf("InitShardMain with mismatched new main returned wrong error: %v", err)
	}

	// run InitShardMain
	if err := wr.InitShardMain(ctx, main.Tablet.Keyspace, main.Tablet.Shard, main.Tablet.Alias, true /*force*/, 10*time.Second); err == nil || !strings.Contains(err.Error(), "wrong input for SetMainCommands") {
		t.Errorf("InitShardMain with one failed subordinate returned wrong error: %v", err)
	}

	// check what was run: main should still be good
	if main.FakeMysqlDaemon.ReadOnly {
		t.Errorf("main was not turned read-write")
	}
	si, err := ts.GetShard(ctx, main.Tablet.Keyspace, main.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if !topoproto.TabletAliasEqual(si.MainAlias, main.Tablet.Alias) {
		t.Errorf("unexpected shard main alias, got %v expected %v", si.MainAlias, main.Tablet.Alias)
	}
}
