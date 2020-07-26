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
	"context"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPlannedReparentShardNoMainProvided(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good subordinates
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate1 := NewFakeTablet(t, wr, "cell2", 2, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteSubordinateResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good subordinate 1 is replicating
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.Replicating = true
	goodSubordinate1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate1.StartActionLoop(t, wr)
	defer goodSubordinate1.StopActionLoop(t)

	// run PlannedReparentShard
	if err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard}); err != nil {
		t.Fatalf("PlannedReparentShard failed: %v", err)
	}

	// // check what was run
	if err := newMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("newMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("oldMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if newMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMain.FakeMysqlDaemon.ReadOnly set")
	}
	if !oldMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMain.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSubordinate1.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.ReadOnly not set")
	}
	if !oldMain.Agent.QueryServiceControl.IsServing() {
		t.Errorf("oldMain...QueryServiceControl not serving")
	}

	// // verify the old main was told to start replicating (and not
	// // the subordinate that wasn't replicating in the first place)
	if !oldMain.FakeMysqlDaemon.Replicating {
		t.Errorf("oldMain.FakeMysqlDaemon.Replicating not set")
	}
	if !goodSubordinate1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.Replicating not set")
	}
	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodSubordinate1, oldMain)
}

func TestPlannedReparentShard(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good subordinates
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteSubordinateResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good subordinate 1 is replicating
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.Replicating = true
	goodSubordinate1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate1.StartActionLoop(t, wr)
	defer goodSubordinate1.StopActionLoop(t)

	// good subordinate 2 is not replicating
	goodSubordinate2.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate2.FakeMysqlDaemon.Replicating = false
	goodSubordinate2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate2.StartActionLoop(t, wr)
	goodSubordinate2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodSubordinate2.StopActionLoop(t)

	// run PlannedReparentShard
	if err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)}); err != nil {
		t.Fatalf("PlannedReparentShard failed: %v", err)
	}

	// check what was run
	if err := newMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("newMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("oldMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Errorf("goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if newMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMain.FakeMysqlDaemon.ReadOnly set")
	}
	if !oldMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMain.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSubordinate1.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSubordinate2.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSubordinate2.FakeMysqlDaemon.ReadOnly not set")
	}
	if !oldMain.Agent.QueryServiceControl.IsServing() {
		t.Errorf("oldMain...QueryServiceControl not serving")
	}

	// verify the old main was told to start replicating (and not
	// the subordinate that wasn't replicating in the first place)
	if !oldMain.FakeMysqlDaemon.Replicating {
		t.Errorf("oldMain.FakeMysqlDaemon.Replicating not set")
	}
	if !goodSubordinate1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.Replicating not set")
	}
	if goodSubordinate2.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSubordinate2.FakeMysqlDaemon.Replicating set")
	}

	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodSubordinate1, goodSubordinate2, oldMain)
}

func TestPlannedReparentNoMain(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a few replicas.
	replica1 := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", replica1.Tablet.Keyspace + "/" + replica1.Tablet.Shard, "-new_main", topoproto.TabletAliasString(replica1.Tablet.Alias)})
	if err == nil {
		t.Fatalf("PlannedReparentShard succeeded: %v", err)
	}
	if !strings.Contains(err.Error(), "the shard has no main") {
		t.Fatalf("PlannedReparentShard failed with the wrong error: %v", err)
	}
}

func TestPlannedReparentShardPromoteSubordinateFail(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good subordinates
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteSubordinateResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	// set to incorrect value to make promote fail on WaitForMainPos
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.PromoteSubordinateResult
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good subordinate 1 is replicating
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.Replicating = true
	goodSubordinate1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate1.StartActionLoop(t, wr)
	defer goodSubordinate1.StopActionLoop(t)

	// good subordinate 2 is not replicating
	goodSubordinate2.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate2.FakeMysqlDaemon.Replicating = false
	goodSubordinate2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate2.StartActionLoop(t, wr)
	goodSubordinate2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodSubordinate2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})

	if err == nil {
		t.Fatalf("PlannedReparentShard succeeded: %v", err)
	}
	if !strings.Contains(err.Error(), "main-elect tablet cell1-0000000001 failed to catch up with replication or be upgraded to main") {
		t.Fatalf("PlannedReparentShard failed with the wrong error: %v", err)
	}

	// now check that DemoteMain was undone and old main is still main
	if !newMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMain.FakeMysqlDaemon.ReadOnly not set")
	}
	if oldMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMain.FakeMysqlDaemon.ReadOnly set")
	}
}

func TestPlannedReparentShardPromoteSubordinateTimeout(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create a main, a couple good subordinates
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate1 := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	goodSubordinate2 := NewFakeTablet(t, wr, "cell2", 3, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.TimeoutHook = func() error { return context.DeadlineExceeded }
	newMain.FakeMysqlDaemon.ReadOnly = true
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.WaitMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   123,
				Sequence: 990,
			},
		},
	}
	newMain.FakeMysqlDaemon.PromoteSubordinateResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   7,
				Server:   456,
				Sequence: 991,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.FakeMysqlDaemon.Replicating = false
	oldMain.FakeMysqlDaemon.CurrentMainPosition = newMain.FakeMysqlDaemon.WaitMainPosition
	oldMain.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	oldMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)
	oldMain.Agent.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)

	// good subordinate 1 is replicating
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.Replicating = true
	goodSubordinate1.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodSubordinate1.StartActionLoop(t, wr)
	defer goodSubordinate1.StopActionLoop(t)

	// good subordinate 2 is not replicating
	goodSubordinate2.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate2.FakeMysqlDaemon.Replicating = false
	goodSubordinate2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate2.StartActionLoop(t, wr)
	goodSubordinate2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodSubordinate2.StopActionLoop(t)

	// run PlannedReparentShard
	err := vp.Run([]string{"PlannedReparentShard", "-wait_subordinate_timeout", "10s", "-keyspace_shard", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, "-new_main", topoproto.TabletAliasString(newMain.Tablet.Alias)})

	if err == nil {
		t.Fatalf("PlannedReparentShard succeeded: %v", err)
	}
	if !strings.Contains(err.Error(), "main-elect tablet cell1-0000000001 failed to catch up with replication or be upgraded to main") {
		t.Fatalf("PlannedReparentShard failed with the wrong error: %v", err)
	}

	// now check that DemoteMain was undone and old main is still main
	if !newMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMain.FakeMysqlDaemon.ReadOnly not set")
	}
	if oldMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("oldMain.FakeMysqlDaemon.ReadOnly set")
	}
}
