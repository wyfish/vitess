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

package vtctl

import (
	"flag"
	"fmt"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func init() {
	addCommand("Tablets", command{
		"ReparentTablet",
		commandReparentTablet,
		"<tablet alias>",
		"Reparent a tablet to the current main in the shard. This only works if the current subordinate position matches the last known reparent action."})

	addCommand("Shards", command{
		"InitShardMain",
		commandInitShardMain,
		"[-force] [-wait_subordinate_timeout=<duration>] <keyspace/shard> <tablet alias>",
		"Sets the initial main for a shard. Will make all other tablets in the shard subordinates of the provided main. WARNING: this could cause data loss on an already replicating shard. PlannedReparentShard or EmergencyReparentShard should be used instead."})
	addCommand("Shards", command{
		"PlannedReparentShard",
		commandPlannedReparentShard,
		"-keyspace_shard=<keyspace/shard> [-new_main=<tablet alias>] [-avoid_main=<tablet alias>]",
		"Reparents the shard to the new main, or away from old main. Both old and new main need to be up and running."})
	addCommand("Shards", command{
		"EmergencyReparentShard",
		commandEmergencyReparentShard,
		"-keyspace_shard=<keyspace/shard> -new_main=<tablet alias>",
		"Reparents the shard to the new main. Assumes the old main is dead and not responsding."})
}

func commandReparentTablet(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the -disable_active_reparents flag to enable)")
	}

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("action ReparentTablet requires <tablet alias>")
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(0))
	if err != nil {
		return err
	}
	return wr.ReparentTablet(ctx, tabletAlias)
}

func commandInitShardMain(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the -disable_active_reparents flag to enable)")
	}

	force := subFlags.Bool("force", false, "will force the reparent even if the provided tablet is not a main or the shard main")
	waitSubordinateTimeout := subFlags.Duration("wait_subordinate_timeout", 30*time.Second, "time to wait for subordinates to catch up in reparenting")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 2 {
		return fmt.Errorf("action InitShardMain requires <keyspace/shard> <tablet alias>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(subFlags.Arg(1))
	if err != nil {
		return err
	}
	return wr.InitShardMain(ctx, keyspace, shard, tabletAlias, *force, *waitSubordinateTimeout)
}

func commandPlannedReparentShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the -disable_active_reparents flag to enable)")
	}

	waitSubordinateTimeout := subFlags.Duration("wait_subordinate_timeout", *topo.RemoteOperationTimeout, "time to wait for subordinates to catch up in reparenting")
	keyspaceShard := subFlags.String("keyspace_shard", "", "keyspace/shard of the shard that needs to be reparented")
	newMain := subFlags.String("new_main", "", "alias of a tablet that should be the new main")
	avoidMain := subFlags.String("avoid_main", "", "alias of a tablet that should not be the main, i.e. reparent to any other tablet if this one is the main")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 2 {
		// Legacy syntax: "<keyspace/shard> <tablet alias>".
		if *keyspaceShard != "" || *newMain != "" {
			return fmt.Errorf("cannot use legacy syntax and flags -keyspace_shard and -new_main for action PlannedReparentShard at the same time")
		}
		*keyspaceShard = subFlags.Arg(0)
		*newMain = subFlags.Arg(1)
	} else if subFlags.NArg() != 0 {
		return fmt.Errorf("action PlannedReparentShard requires -keyspace_shard=<keyspace/shard> [-new_main=<tablet alias>] [-avoid_main=<tablet alias>]")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(*keyspaceShard)
	if err != nil {
		return err
	}
	var newMainAlias, avoidMainAlias *topodatapb.TabletAlias
	if *newMain != "" {
		newMainAlias, err = topoproto.ParseTabletAlias(*newMain)
		if err != nil {
			return err
		}
	}
	if *avoidMain != "" {
		avoidMainAlias, err = topoproto.ParseTabletAlias(*avoidMain)
		if err != nil {
			return err
		}
	}
	return wr.PlannedReparentShard(ctx, keyspace, shard, newMainAlias, avoidMainAlias, *waitSubordinateTimeout)
}

func commandEmergencyReparentShard(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if *mysqlctl.DisableActiveReparents {
		return fmt.Errorf("active reparent commands disabled (unset the -disable_active_reparents flag to enable)")
	}

	waitSubordinateTimeout := subFlags.Duration("wait_subordinate_timeout", 30*time.Second, "time to wait for subordinates to catch up in reparenting")
	keyspaceShard := subFlags.String("keyspace_shard", "", "keyspace/shard of the shard that needs to be reparented")
	newMain := subFlags.String("new_main", "", "alias of a tablet that should be the new main")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() == 2 {
		// Legacy syntax: "<keyspace/shard> <tablet alias>".
		if *newMain != "" {
			return fmt.Errorf("cannot use legacy syntax and flag -new_main for action EmergencyReparentShard at the same time")
		}
		*keyspaceShard = subFlags.Arg(0)
		*newMain = subFlags.Arg(1)
	} else if subFlags.NArg() != 0 {
		return fmt.Errorf("action EmergencyReparentShard requires -keyspace_shard=<keyspace/shard> -new_main=<tablet alias>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(*keyspaceShard)
	if err != nil {
		return err
	}
	tabletAlias, err := topoproto.ParseTabletAlias(*newMain)
	if err != nil {
		return err
	}
	return wr.EmergencyReparentShard(ctx, keyspace, shard, tabletAlias, *waitSubordinateTimeout)
}
