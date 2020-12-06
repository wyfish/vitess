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
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestEmergencyReparentShard(t *testing.T) {
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
	newMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, main_alias, replication_position) VALUES",
	}
	newMain.FakeMysqlDaemon.PromoteSubordinateResult = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main, will be scrapped
	oldMain.FakeMysqlDaemon.ReadOnly = false
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// good subordinate 1 is replicating
	goodSubordinate1.FakeMysqlDaemon.ReadOnly = true
	goodSubordinate1.FakeMysqlDaemon.Replicating = true
	goodSubordinate1.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 455,
			},
		},
	}
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
	goodSubordinate2.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 454,
			},
		},
	}
	goodSubordinate2.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(newMain.Tablet)
	goodSubordinate2.StartActionLoop(t, wr)
	goodSubordinate2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	defer goodSubordinate2.StopActionLoop(t)

	// run EmergencyReparentShard
	if err := vp.Run([]string{"EmergencyReparentShard", "-wait_subordinate_timeout", "10s", newMain.Tablet.Keyspace + "/" + newMain.Tablet.Shard, topoproto.TabletAliasString(newMain.Tablet.Alias)}); err != nil {
		t.Fatalf("EmergencyReparentShard failed: %v", err)
	}

	// check what was run
	if err := newMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("newMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("oldMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSubordinate1.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("goodSubordinate2.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if newMain.FakeMysqlDaemon.ReadOnly {
		t.Errorf("newMain.FakeMysqlDaemon.ReadOnly set")
	}
	// old main read-only flag doesn't matter, it is scrapped
	if !goodSubordinate1.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSubordinate2.FakeMysqlDaemon.ReadOnly {
		t.Errorf("goodSubordinate2.FakeMysqlDaemon.ReadOnly not set")
	}
	if !goodSubordinate1.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSubordinate1.FakeMysqlDaemon.Replicating not set")
	}
	if goodSubordinate2.FakeMysqlDaemon.Replicating {
		t.Errorf("goodSubordinate2.FakeMysqlDaemon.Replicating set")
	}
	checkSemiSyncEnabled(t, true, true, newMain)
	checkSemiSyncEnabled(t, false, true, goodSubordinate1, goodSubordinate2)
}

// TestEmergencyReparentShardMainElectNotBest tries to emergency reparent
// to a host that is not the latest in replication position.
func TestEmergencyReparentShardMainElectNotBest(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create a main, a couple good subordinates
	oldMain := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMain := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	moreAdvancedSubordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// new main
	newMain.FakeMysqlDaemon.Replicating = true
	newMain.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 456,
			},
		},
	}
	newMain.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	newMain.StartActionLoop(t, wr)
	defer newMain.StopActionLoop(t)

	// old main, will be scrapped
	oldMain.StartActionLoop(t, wr)
	defer oldMain.StopActionLoop(t)

	// more advanced subordinate
	moreAdvancedSubordinate.FakeMysqlDaemon.Replicating = true
	moreAdvancedSubordinate.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   2,
				Server:   123,
				Sequence: 457,
			},
		},
	}
	moreAdvancedSubordinate.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
	}
	moreAdvancedSubordinate.StartActionLoop(t, wr)
	defer moreAdvancedSubordinate.StopActionLoop(t)

	// run EmergencyReparentShard
	if err := wr.EmergencyReparentShard(ctx, newMain.Tablet.Keyspace, newMain.Tablet.Shard, newMain.Tablet.Alias, 10*time.Second); err == nil || !strings.Contains(err.Error(), "is more advanced than main elect tablet") {
		t.Fatalf("EmergencyReparentShard returned the wrong error: %v", err)
	}

	// check what was run
	if err := newMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("newMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := oldMain.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("oldMain.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	if err := moreAdvancedSubordinate.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("moreAdvancedSubordinate.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
}
