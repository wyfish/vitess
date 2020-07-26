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
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestShardReplicationStatuses(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("GetOrCreateShard failed: %v", err)
	}
	main := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, nil)
	subordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// mark the main inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.MainAlias = main.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// main action loop (to initialize host and port)
	main.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)

	// subordinate loop
	subordinate.FakeMysqlDaemon.CurrentMainPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	subordinate.FakeMysqlDaemon.CurrentMainHost = topoproto.MysqlHostname(main.Tablet)
	subordinate.FakeMysqlDaemon.CurrentMainPort = int(topoproto.MysqlPort(main.Tablet))
	subordinate.StartActionLoop(t, wr)
	defer subordinate.StopActionLoop(t)

	// run ShardReplicationStatuses
	ti, rs, err := wr.ShardReplicationStatuses(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("ShardReplicationStatuses failed: %v", err)
	}

	// check result (make main first in the array)
	if len(ti) != 2 || len(rs) != 2 {
		t.Fatalf("ShardReplicationStatuses returned wrong results: %v %v", ti, rs)
	}
	if topoproto.TabletAliasEqual(ti[0].Alias, subordinate.Tablet.Alias) {
		ti[0], ti[1] = ti[1], ti[0]
		rs[0], rs[1] = rs[1], rs[0]
	}
	if !topoproto.TabletAliasEqual(ti[0].Alias, main.Tablet.Alias) ||
		!topoproto.TabletAliasEqual(ti[1].Alias, subordinate.Tablet.Alias) ||
		rs[0].MainHost != "" ||
		rs[1].MainHost != main.Tablet.Hostname {
		t.Fatalf("ShardReplicationStatuses returend wrong results: %v %v", ti, rs)
	}
}

func TestReparentTablet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	main := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, nil)
	subordinate := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// mark the main inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.MainAlias = main.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// main action loop (to initialize host and port)
	main.StartActionLoop(t, wr)
	defer main.StopActionLoop(t)

	// subordinate loop
	subordinate.FakeMysqlDaemon.SetMainInput = topoproto.MysqlAddr(main.Tablet)
	subordinate.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
	}
	subordinate.StartActionLoop(t, wr)
	defer subordinate.StopActionLoop(t)

	// run ReparentTablet
	if err := wr.ReparentTablet(ctx, subordinate.Tablet.Alias); err != nil {
		t.Fatalf("ReparentTablet failed: %v", err)
	}

	// check what was run
	if err := subordinate.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("subordinate.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	checkSemiSyncEnabled(t, false, true, subordinate)
}
