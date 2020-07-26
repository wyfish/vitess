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

package wrangler

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// As with all distributed systems, things can skew. These functions
// explore data in topology server and attempt to square that with reality.
//
// Given the node counts are usually large, this work should be done
// with as much parallelism as is viable.
//
// This may eventually move into a separate package.

// waitForResults will wait for all the errors to come back.
// There is no timeout, as individual calls will use the context and timeout
// and fail at the end anyway.
func (wr *Wrangler) waitForResults(wg *sync.WaitGroup, results chan error) error {
	go func() {
		wg.Wait()
		close(results)
	}()

	var finalErr error
	for err := range results {
		finalErr = errors.New("some validation errors - see log")
		wr.Logger().Error(err)
	}
	return finalErr
}

// Validate all tablets in all discoverable cells, even if they are
// not in the replication graph.
func (wr *Wrangler) validateAllTablets(ctx context.Context, wg *sync.WaitGroup, results chan<- error) {
	cellSet := make(map[string]bool, 16)

	keyspaces, err := wr.ts.GetKeyspaces(ctx)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetKeyspaces failed: %v", err)
		return
	}
	for _, keyspace := range keyspaces {
		shards, err := wr.ts.GetShardNames(ctx, keyspace)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
			return
		}

		for _, shard := range shards {
			aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
			if err != nil {
				results <- fmt.Errorf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
				return
			}
			for _, alias := range aliases {
				cellSet[alias.Cell] = true
			}
		}
	}

	for cell := range cellSet {
		aliases, err := wr.ts.GetTabletsByCell(ctx, cell)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetTabletsByCell(%v) failed: %v", cell, err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go func(alias *topodatapb.TabletAlias) {
				defer wg.Done()
				if err := topo.Validate(ctx, wr.ts, alias); err != nil {
					results <- fmt.Errorf("topo.Validate(%v) failed: %v", topoproto.TabletAliasString(alias), err)
				} else {
					wr.Logger().Infof("tablet %v is valid", topoproto.TabletAliasString(alias))
				}
			}(alias)
		}
	}
}

func (wr *Wrangler) validateKeyspace(ctx context.Context, keyspace string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	// Validate replication graph by traversing each shard.
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
		return
	}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			wr.validateShard(ctx, keyspace, shard, pingTablets, wg, results)
		}(shard)
	}
}

func (wr *Wrangler) validateShard(ctx context.Context, keyspace, shard string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	tabletMap, _ := wr.ts.GetTabletMap(ctx, aliases)

	var mainAlias *topodatapb.TabletAlias
	for _, alias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(alias)]
		if !ok {
			results <- fmt.Errorf("tablet %v not found in map", topoproto.TabletAliasString(alias))
			continue
		}
		if tabletInfo.Type == topodatapb.TabletType_MASTER {
			if mainAlias != nil {
				results <- fmt.Errorf("shard %v/%v already has main %v but found other main %v", keyspace, shard, topoproto.TabletAliasString(mainAlias), topoproto.TabletAliasString(alias))
			} else {
				mainAlias = alias
			}
		}
	}

	if mainAlias == nil {
		results <- fmt.Errorf("no main for shard %v/%v", keyspace, shard)
	} else if !topoproto.TabletAliasEqual(shardInfo.MainAlias, mainAlias) {
		results <- fmt.Errorf("main mismatch for shard %v/%v: found %v, expected %v", keyspace, shard, topoproto.TabletAliasString(mainAlias), topoproto.TabletAliasString(shardInfo.MainAlias))
	}

	for _, alias := range aliases {
		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()
			if err := topo.Validate(ctx, wr.ts, alias); err != nil {
				results <- fmt.Errorf("topo.Validate(%v) failed: %v", topoproto.TabletAliasString(alias), err)
			} else {
				wr.Logger().Infof("tablet %v is valid", topoproto.TabletAliasString(alias))
			}
		}(alias)
	}

	if pingTablets {
		wr.validateReplication(ctx, shardInfo, tabletMap, results)
		wr.pingTablets(ctx, tabletMap, wg, results)
	}
}

func normalizeIP(ip string) string {
	// Normalize loopback to avoid spurious validation errors.
	if parsedIP := net.ParseIP(ip); parsedIP != nil && parsedIP.IsLoopback() {
		// Note that this also maps IPv6 localhost to IPv4 localhost
		// as GetSubordinates() will return only IPv4 addresses.
		return "127.0.0.1"
	}
	return ip
}

func (wr *Wrangler) validateReplication(ctx context.Context, shardInfo *topo.ShardInfo, tabletMap map[string]*topo.TabletInfo, results chan<- error) {
	if shardInfo.MainAlias == nil {
		results <- fmt.Errorf("no main in shard record %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())
		return
	}

	shardInfoMainAliasStr := topoproto.TabletAliasString(shardInfo.MainAlias)
	mainTabletInfo, ok := tabletMap[shardInfoMainAliasStr]
	if !ok {
		results <- fmt.Errorf("main %v not in tablet map", shardInfoMainAliasStr)
		return
	}

	subordinateList, err := wr.tmc.GetSubordinates(ctx, mainTabletInfo.Tablet)
	if err != nil {
		results <- fmt.Errorf("GetSubordinates(%v) failed: %v", mainTabletInfo, err)
		return
	}
	if len(subordinateList) == 0 {
		results <- fmt.Errorf("no subordinates of tablet %v found", shardInfoMainAliasStr)
		return
	}

	tabletIPMap := make(map[string]*topodatapb.Tablet)
	subordinateIPMap := make(map[string]bool)
	for _, tablet := range tabletMap {
		ip, err := topoproto.MySQLIP(tablet.Tablet)
		if err != nil {
			results <- fmt.Errorf("could not resolve IP for tablet %s: %v", topoproto.MysqlHostname(tablet.Tablet), err)
			continue
		}
		tabletIPMap[normalizeIP(ip)] = tablet.Tablet
	}

	// See if every subordinate is in the replication graph.
	for _, subordinateAddr := range subordinateList {
		if tabletIPMap[normalizeIP(subordinateAddr)] == nil {
			results <- fmt.Errorf("subordinate %v not in replication graph for shard %v/%v (mysql instance without vttablet?)", subordinateAddr, shardInfo.Keyspace(), shardInfo.ShardName())
		}
		subordinateIPMap[normalizeIP(subordinateAddr)] = true
	}

	// See if every entry in the replication graph is connected to the main.
	for _, tablet := range tabletMap {
		if !tablet.IsSubordinateType() {
			continue
		}

		ip, err := topoproto.MySQLIP(tablet.Tablet)
		if err != nil {
			results <- fmt.Errorf("could not resolve IP for tablet %s: %v", topoproto.MysqlHostname(tablet.Tablet), err)
			continue
		}
		if !subordinateIPMap[normalizeIP(ip)] {
			results <- fmt.Errorf("subordinate %v not replicating: %v subordinate list: %q", topoproto.TabletAliasString(tablet.Alias), ip, subordinateList)
		}
	}
}

func (wr *Wrangler) pingTablets(ctx context.Context, tabletMap map[string]*topo.TabletInfo, wg *sync.WaitGroup, results chan<- error) {
	for tabletAlias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(tabletAlias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()

			if err := wr.tmc.Ping(ctx, tabletInfo.Tablet); err != nil {
				//lint:ignore ST1005 function name
				results <- fmt.Errorf("Ping(%v) failed: %v tablet hostname: %v", tabletAlias, err, tabletInfo.Hostname)
			}
		}(tabletAlias, tabletInfo)
	}
}

// Validate a whole TopologyServer tree
func (wr *Wrangler) Validate(ctx context.Context, pingTablets bool) error {
	// Results from various actions feed here.
	results := make(chan error, 16)
	wg := &sync.WaitGroup{}

	// Validate all tablets in all cells, even if they are not discoverable
	// by the replication graph.
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateAllTablets(ctx, wg, results)
	}()

	// Validate replication graph by traversing each keyspace and then each shard.
	keyspaces, err := wr.ts.GetKeyspaces(ctx)
	if err != nil {
		results <- fmt.Errorf("GetKeyspaces failed: %v", err)
	} else {
		for _, keyspace := range keyspaces {
			wg.Add(1)
			go func(keyspace string) {
				defer wg.Done()
				wr.validateKeyspace(ctx, keyspace, pingTablets, wg, results)
			}(keyspace)
		}
	}
	return wr.waitForResults(wg, results)
}

// ValidateKeyspace will validate a bunch of information in a keyspace
// is correct.
func (wr *Wrangler) ValidateKeyspace(ctx context.Context, keyspace string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateKeyspace(ctx, keyspace, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}

// ValidateShard will validate a bunch of information in a shard is correct.
func (wr *Wrangler) ValidateShard(ctx context.Context, keyspace, shard string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateShard(ctx, keyspace, shard, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}
