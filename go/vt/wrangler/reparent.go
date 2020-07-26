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

/*
This file handles the reparenting operations.
*/

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	initShardMainOperation        = "InitShardMain"
	plannedReparentShardOperation   = "PlannedReparentShard"
	emergencyReparentShardOperation = "EmergencyReparentShard"
)

// ShardReplicationStatuses returns the ReplicationStatus for each tablet in a shard.
func (wr *Wrangler) ShardReplicationStatuses(ctx context.Context, keyspace, shard string) ([]*topo.TabletInfo, []*replicationdatapb.Status, error) {
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return nil, nil, err
	}
	tablets := topotools.CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)

	wr.logger.Infof("Gathering tablet replication status for: %v", tablets)
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	result := make([]*replicationdatapb.Status, len(tablets))

	for i, ti := range tablets {
		// Don't scan tablets that won't return something
		// useful. Otherwise, you'll end up waiting for a timeout.
		if ti.Type == topodatapb.TabletType_MASTER {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				pos, err := wr.tmc.MainPosition(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("MainPosition(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = &replicationdatapb.Status{
					Position: pos,
				}
			}(i, ti)
		} else if ti.IsSubordinateType() {
			wg.Add(1)
			go func(i int, ti *topo.TabletInfo) {
				defer wg.Done()
				status, err := wr.tmc.SubordinateStatus(ctx, ti.Tablet)
				if err != nil {
					rec.RecordError(fmt.Errorf("SubordinateStatus(%v) failed: %v", ti.AliasString(), err))
					return
				}
				result[i] = status
			}(i, ti)
		}
	}
	wg.Wait()
	return tablets, result, rec.Error()
}

// ReparentTablet tells a tablet to reparent this tablet to the current
// main, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	// Get specified tablet.
	// Get current shard main tablet.
	// Sanity check they are in the same keyspace/shard.
	// Issue a SetMain to the tablet.
	ti, err := wr.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	shardInfo, err := wr.ts.GetShard(ctx, ti.Keyspace, ti.Shard)
	if err != nil {
		return err
	}
	if !shardInfo.HasMain() {
		return fmt.Errorf("no main tablet for shard %v/%v", ti.Keyspace, ti.Shard)
	}

	mainTi, err := wr.ts.GetTablet(ctx, shardInfo.MainAlias)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if mainTi.Type != topodatapb.TabletType_MASTER {
		return fmt.Errorf("TopologyServer has inconsistent state for shard main %v", topoproto.TabletAliasString(shardInfo.MainAlias))
	}
	if mainTi.Keyspace != ti.Keyspace || mainTi.Shard != ti.Shard {
		return fmt.Errorf("main %v and potential subordinate not in same keyspace/shard", topoproto.TabletAliasString(shardInfo.MainAlias))
	}

	// and do the remote command
	return wr.tmc.SetMain(ctx, ti.Tablet, shardInfo.MainAlias, 0, false)
}

// InitShardMain will make the provided tablet the main for the shard.
func (wr *Wrangler) InitShardMain(ctx context.Context, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, force bool, waitSubordinateTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("InitShardMain(%v)", topoproto.TabletAliasString(mainElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.initShardMainLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, force, waitSubordinateTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed InitShardMain: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished InitShardMain")
	}
	return err
}

func (wr *Wrangler) initShardMainLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, force bool, waitSubordinateTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check the main elect is in tabletMap.
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	ev.NewMain = *mainElectTabletInfo.Tablet

	// Check the main is the only main is the shard, or -force was used.
	_, mainTabletMap := topotools.SortedTabletMap(tabletMap)
	if !topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not the shard main, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not the shard main, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	if _, ok := mainTabletMap[mainElectTabletAliasStr]; !ok {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not a main in the shard, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not a main in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	haveOtherMain := false
	for alias := range mainTabletMap {
		if mainElectTabletAliasStr != alias {
			haveOtherMain = true
		}
	}
	if haveOtherMain {
		if !force {
			return fmt.Errorf("main-elect tablet %v is not the only main in the shard, use -force to proceed anyway", topoproto.TabletAliasString(mainElectTabletAlias))
		}
		wr.logger.Warningf("main-elect tablet %v is not the only main in the shard, proceeding anyway as -force was used", topoproto.TabletAliasString(mainElectTabletAlias))
	}

	// First phase: reset replication on all tablets. If anyone fails,
	// we stop. It is probably because it is unreachable, and may leave
	// an unstable database process in the mix, with a database daemon
	// at a wrong replication spot.

	// Create a context for the following RPCs that respects waitSubordinateTimeout
	resetCtx, resetCancel := context.WithTimeout(ctx, waitSubordinateTimeout)
	defer resetCancel()

	event.DispatchUpdate(ev, "resetting replication on all tablets")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("resetting replication on tablet %v", alias)
			if err := wr.tmc.ResetReplication(resetCtx, tabletInfo.Tablet); err != nil {
				rec.RecordError(fmt.Errorf("tablet %v ResetReplication failed (either fix it, or Scrap it): %v", alias, err))
			}
		}(alias, tabletInfo)
	}
	wg.Wait()
	if err := rec.Error(); err != nil {
		// if any of the subordinates failed
		return err
	}

	// Check we still have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Tell the new main to break its subordinates, return its replication
	// position
	wr.logger.Infof("initializing main on %v", topoproto.TabletAliasString(mainElectTabletAlias))
	event.DispatchUpdate(ev, "initializing main")
	rp, err := wr.tmc.InitMain(ctx, mainElectTabletInfo.Tablet)
	if err != nil {
		return err
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitSubordinateTimeout)
	defer replCancel()

	// Now tell the new main to insert the reparent_journal row,
	// and tell everybody else to become a subordinate of the new main,
	// and wait for the row in the reparent_journal table.
	// We start all these in parallel, to handle the semi-sync
	// case: for the main to be able to commit its row in the
	// reparent_journal table, it needs connected subordinates.
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMain := sync.WaitGroup{}
	wgSubordinates := sync.WaitGroup{}
	var mainErr error
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			wgMain.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMain.Done()
				wr.logger.Infof("populating reparent journal on new main %v", alias)
				mainErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, initShardMainOperation, mainElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSubordinates.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSubordinates.Done()
				wr.logger.Infof("initializing subordinate %v", alias)
				if err := wr.tmc.InitSubordinate(replCtx, tabletInfo.Tablet, mainElectTabletAlias, rp, now); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v InitSubordinate failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the main is done, we can update the shard record
	// (note with semi-sync, it also means at least one subordinate is done).
	wgMain.Wait()
	if mainErr != nil {
		// The main failed, there is no way the
		// subordinates will work.  So we cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling subordinates")
		replCancel()
		wgSubordinates.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", mainErr)
	}
	if !topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.MainAlias = mainElectTabletAlias
			return nil
		}); err != nil {
			wgSubordinates.Wait()
			return fmt.Errorf("failed to update shard main record: %v", err)
		}
	}

	// Wait for the subordinates to complete. If some of them fail, we
	// don't want to rebuild the shard serving graph (the failure
	// will most likely be a timeout, and our context will be
	// expired, so the rebuild will fail anyway)
	wgSubordinates.Wait()
	if err := rec.Error(); err != nil {
		return err
	}

	// Create database if necessary on the main. Subordinates will get it too through
	// replication. Since the user called InitShardMain, they've told us to
	// assume that whatever data is on all the subordinates is what they intended.
	// If the database doesn't exist, it means the user intends for these tablets
	// to begin serving with no data (i.e. first time initialization).
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeID(topoproto.TabletDbName(mainElectTabletInfo.Tablet)))
	if _, err := wr.tmc.ExecuteFetchAsDba(ctx, mainElectTabletInfo.Tablet, false, []byte(createDB), 1, false, true); err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}

	return nil
}

// PlannedReparentShard will make the provided tablet the main for the shard,
// when both the current and new main are reachable and in good shape.
func (wr *Wrangler) PlannedReparentShard(ctx context.Context, keyspace, shard string, mainElectTabletAlias, avoidMainAlias *topodatapb.TabletAlias, waitSubordinateTimeout time.Duration) (err error) {
	// lock the shard
	lockAction := fmt.Sprintf(
		"PlannedReparentShard(%v, avoid_main=%v)",
		topoproto.TabletAliasString(mainElectTabletAlias),
		topoproto.TabletAliasString(avoidMainAlias))
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, lockAction)
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// Attempt to set avoidMainAlias if not provided by parameters
	if mainElectTabletAlias == nil && avoidMainAlias == nil {
		shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return err
		}
		avoidMainAlias = shardInfo.MainAlias
	}

	// do the work
	err = wr.plannedReparentShardLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, avoidMainAlias, waitSubordinateTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed PlannedReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished PlannedReparentShard")
	}
	return err
}

func (wr *Wrangler) plannedReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias, avoidMainTabletAlias *topodatapb.TabletAlias, waitSubordinateTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading tablet map")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check corner cases we're going to depend on
	if topoproto.TabletAliasEqual(mainElectTabletAlias, avoidMainTabletAlias) {
		return fmt.Errorf("main-elect tablet %v is the same as the tablet to avoid", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	if mainElectTabletAlias == nil {
		if !topoproto.TabletAliasEqual(avoidMainTabletAlias, shardInfo.MainAlias) {
			event.DispatchUpdate(ev, "current main is different than -avoid_main, nothing to do")
			return nil
		}
		event.DispatchUpdate(ev, "searching for main candidate")
		mainElectTabletAlias, err = wr.chooseNewMain(ctx, shardInfo, tabletMap, avoidMainTabletAlias, waitSubordinateTimeout)
		if err != nil {
			return err
		}
		if mainElectTabletAlias == nil {
			return fmt.Errorf("cannot find a tablet to reparent to")
		}
		wr.logger.Infof("elected new main candidate %v", topoproto.TabletAliasString(mainElectTabletAlias))
		event.DispatchUpdate(ev, "elected new main candidate")
	}
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", mainElectTabletAliasStr)
	}
	ev.NewMain = *mainElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		return fmt.Errorf("main-elect tablet %v is already the main", mainElectTabletAliasStr)
	}
	if topoproto.TabletAliasIsZero(shardInfo.MainAlias) {
		return fmt.Errorf("the shard has no main, use EmergencyReparentShard")
	}
	oldMainTabletInfo, ok := tabletMap[topoproto.TabletAliasString(shardInfo.MainAlias)]
	if !ok {
		return fmt.Errorf("old main tablet %v is not in the shard", topoproto.TabletAliasString(shardInfo.MainAlias))
	}
	ev.OldMain = *oldMainTabletInfo.Tablet

	// create a new context for the short running remote operations
	remoteCtx, remoteCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer remoteCancel()

	// Demote the current main, get its replication position
	wr.logger.Infof("demote current main %v", shardInfo.MainAlias)
	event.DispatchUpdate(ev, "demoting old main")
	rp, err := wr.tmc.DemoteMain(remoteCtx, oldMainTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("old main tablet %v DemoteMain failed: %v", topoproto.TabletAliasString(shardInfo.MainAlias), err)
	}

	remoteCtx, remoteCancel = context.WithTimeout(ctx, waitSubordinateTimeout)
	defer remoteCancel()

	// Wait on the main-elect tablet until it reaches that position,
	// then promote it
	wr.logger.Infof("promote subordinate %v", mainElectTabletAliasStr)
	event.DispatchUpdate(ev, "promoting subordinate")
	rp, err = wr.tmc.PromoteSubordinateWhenCaughtUp(remoteCtx, mainElectTabletInfo.Tablet, rp)
	if err != nil || (ctx.Err() != nil && ctx.Err() == context.DeadlineExceeded) {
		remoteCancel()
		// if this fails it is not enough to return an error. we should rollback all the changes made by DemoteMain
		remoteCtx, remoteCancel = context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer remoteCancel()
		if err1 := wr.tmc.UndoDemoteMain(remoteCtx, oldMainTabletInfo.Tablet); err1 != nil {
			log.Warningf("Encountered error %v while trying to undo DemoteMain", err1)
		}
		return fmt.Errorf("main-elect tablet %v failed to catch up with replication or be upgraded to main: %v", mainElectTabletAliasStr, err)
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithTimeout(ctx, waitSubordinateTimeout)
	defer replCancel()

	// Go through all the tablets:
	// - new main: populate the reparent journal
	// - everybody else: reparent to new main, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMain := sync.WaitGroup{}
	wgSubordinates := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var mainErr error
	oldMainTabletInfoAliasStr := topoproto.TabletAliasString(oldMainTabletInfo.Alias)
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			wgMain.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMain.Done()
				wr.logger.Infof("populating reparent journal on new main %v", alias)
				mainErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, plannedReparentShardOperation, mainElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSubordinates.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSubordinates.Done()
				wr.logger.Infof("setting new main on subordinate %v", alias)
				// also restart replication on old main
				forceStartSubordinate := alias == oldMainTabletInfoAliasStr
				if err := wr.tmc.SetMain(replCtx, tabletInfo.Tablet, mainElectTabletAlias, now, forceStartSubordinate); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v SetMain failed: %v", alias, err))
					return
				}
			}(alias, tabletInfo)
		}
	}

	// After the main is done, we can update the shard record
	// (note with semi-sync, it also means at least one subordinate is done)
	wgMain.Wait()
	if mainErr != nil {
		// The main failed, there is no way the
		// subordinates will work.  So we cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling subordinates")
		replCancel()
		wgSubordinates.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", mainErr)
	}
	wr.logger.Infof("updating shard record with new main %v", mainElectTabletAlias)
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MainAlias = mainElectTabletAlias
		return nil
	}); err != nil {
		wgSubordinates.Wait()
		return fmt.Errorf("failed to update shard main record: %v", err)
	}

	// Wait for the subordinates to complete.
	wgSubordinates.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some subordinates failed to reparent")
		return err
	}

	return nil
}

// maxReplPosSearch is a struct helping to search for a tablet with the largest replication
// position querying status from all tablets in parallel.
type maxReplPosSearch struct {
	wrangler         *Wrangler
	ctx              context.Context
	waitSubordinateTimeout time.Duration
	waitGroup        sync.WaitGroup
	maxPosLock       sync.Mutex
	maxPos           mysql.Position
	maxPosTablet     *topodatapb.Tablet
}

func (maxPosSearch *maxReplPosSearch) processTablet(tablet *topodatapb.Tablet) {
	defer maxPosSearch.waitGroup.Done()
	maxPosSearch.wrangler.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	subordinateStatusCtx, cancelSubordinateStatus := context.WithTimeout(maxPosSearch.ctx, maxPosSearch.waitSubordinateTimeout)
	defer cancelSubordinateStatus()

	status, err := maxPosSearch.wrangler.tmc.SubordinateStatus(subordinateStatusCtx, tablet)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)
		return
	}
	replPos, err := mysql.DecodePosition(status.Position)
	if err != nil {
		maxPosSearch.wrangler.logger.Warningf("cannot decode subordinate %v position %v: %v", topoproto.TabletAliasString(tablet.Alias), status.Position, err)
		return
	}

	maxPosSearch.maxPosLock.Lock()
	if maxPosSearch.maxPosTablet == nil || !maxPosSearch.maxPos.AtLeast(replPos) {
		maxPosSearch.maxPos = replPos
		maxPosSearch.maxPosTablet = tablet
	}
	maxPosSearch.maxPosLock.Unlock()
}

// chooseNewMain finds a tablet that is going to become main after reparent. The criteria
// for the new main-elect are (preferably) to be in the same cell as the current main, and
// to be different from avoidMainTabletAlias. The tablet with the largest replication
// position is chosen to minimize the time of catching up with the main. Note that the search
// for largest replication position will race with transactions being executed on the main at
// the same time, so when all tablets are roughly at the same position then the choice of the
// new main-elect will be somewhat unpredictable.
func (wr *Wrangler) chooseNewMain(
	ctx context.Context,
	shardInfo *topo.ShardInfo,
	tabletMap map[string]*topo.TabletInfo,
	avoidMainTabletAlias *topodatapb.TabletAlias,
	waitSubordinateTimeout time.Duration) (*topodatapb.TabletAlias, error) {

	if avoidMainTabletAlias == nil {
		return nil, fmt.Errorf("tablet to avoid for reparent is not provided, cannot choose new main")
	}
	var mainCell string
	if shardInfo.MainAlias != nil {
		mainCell = shardInfo.MainAlias.Cell
	}

	maxPosSearch := maxReplPosSearch{
		wrangler:         wr,
		ctx:              ctx,
		waitSubordinateTimeout: waitSubordinateTimeout,
		waitGroup:        sync.WaitGroup{},
		maxPosLock:       sync.Mutex{},
	}
	for _, tabletInfo := range tabletMap {
		if (mainCell != "" && tabletInfo.Alias.Cell != mainCell) ||
			topoproto.TabletAliasEqual(tabletInfo.Alias, avoidMainTabletAlias) ||
			tabletInfo.Tablet.Type != topodatapb.TabletType_REPLICA {
			continue
		}
		maxPosSearch.waitGroup.Add(1)
		go maxPosSearch.processTablet(tabletInfo.Tablet)
	}
	maxPosSearch.waitGroup.Wait()

	if maxPosSearch.maxPosTablet == nil {
		return nil, nil
	}
	return maxPosSearch.maxPosTablet.Alias, nil
}

// EmergencyReparentShard will make the provided tablet the main for
// the shard, when the old main is completely unreachable.
func (wr *Wrangler) EmergencyReparentShard(ctx context.Context, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, waitSubordinateTimeout time.Duration) (err error) {
	// lock the shard
	ctx, unlock, lockErr := wr.ts.LockShard(ctx, keyspace, shard, fmt.Sprintf("EmergencyReparentShard(%v)", topoproto.TabletAliasString(mainElectTabletAlias)))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// Create reusable Reparent event with available info
	ev := &events.Reparent{}

	// do the work
	err = wr.emergencyReparentShardLocked(ctx, ev, keyspace, shard, mainElectTabletAlias, waitSubordinateTimeout)
	if err != nil {
		event.DispatchUpdate(ev, "failed EmergencyReparentShard: "+err.Error())
	} else {
		event.DispatchUpdate(ev, "finished EmergencyReparentShard")
	}
	return err
}

func (wr *Wrangler) emergencyReparentShardLocked(ctx context.Context, ev *events.Reparent, keyspace, shard string, mainElectTabletAlias *topodatapb.TabletAlias, waitSubordinateTimeout time.Duration) error {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	ev.ShardInfo = *shardInfo

	event.DispatchUpdate(ev, "reading all tablets")
	tabletMap, err := wr.ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	// Check corner cases we're going to depend on
	mainElectTabletAliasStr := topoproto.TabletAliasString(mainElectTabletAlias)
	mainElectTabletInfo, ok := tabletMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("main-elect tablet %v is not in the shard", mainElectTabletAliasStr)
	}
	ev.NewMain = *mainElectTabletInfo.Tablet
	if topoproto.TabletAliasEqual(shardInfo.MainAlias, mainElectTabletAlias) {
		return fmt.Errorf("main-elect tablet %v is already the main", topoproto.TabletAliasString(mainElectTabletAlias))
	}

	// Deal with the old main: try to remote-scrap it, if it's
	// truly dead we force-scrap it. Remove it from our map in any case.
	if shardInfo.HasMain() {
		deleteOldMain := true
		shardInfoMainAliasStr := topoproto.TabletAliasString(shardInfo.MainAlias)
		oldMainTabletInfo, ok := tabletMap[shardInfoMainAliasStr]
		if ok {
			delete(tabletMap, shardInfoMainAliasStr)
		} else {
			oldMainTabletInfo, err = wr.ts.GetTablet(ctx, shardInfo.MainAlias)
			if err != nil {
				wr.logger.Warningf("cannot read old main tablet %v, won't touch it: %v", shardInfoMainAliasStr, err)
				deleteOldMain = false
			}
		}

		if deleteOldMain {
			ev.OldMain = *oldMainTabletInfo.Tablet
			wr.logger.Infof("deleting old main %v", shardInfoMainAliasStr)

			ctx, cancel := context.WithTimeout(ctx, waitSubordinateTimeout)
			defer cancel()

			if err := topotools.DeleteTablet(ctx, wr.ts, oldMainTabletInfo.Tablet); err != nil {
				wr.logger.Warningf("failed to delete old main tablet %v: %v", shardInfoMainAliasStr, err)
			}
		}
	}

	// Stop replication on all subordinates, get their current
	// replication position
	event.DispatchUpdate(ev, "stop replication on all subordinates")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	statusMap := make(map[string]*replicationdatapb.Status)
	for alias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(alias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()
			wr.logger.Infof("getting replication position from %v", alias)
			ctx, cancel := context.WithTimeout(ctx, waitSubordinateTimeout)
			defer cancel()
			rp, err := wr.tmc.StopReplicationAndGetStatus(ctx, tabletInfo.Tablet)
			if err != nil {
				wr.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", alias, err)
				return
			}
			mu.Lock()
			statusMap[alias] = rp
			mu.Unlock()
		}(alias, tabletInfo)
	}
	wg.Wait()

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Verify mainElect is alive and has the most advanced position
	mainElectStatus, ok := statusMap[mainElectTabletAliasStr]
	if !ok {
		return fmt.Errorf("couldn't get main elect %v replication position", topoproto.TabletAliasString(mainElectTabletAlias))
	}
	mainElectPos, err := mysql.DecodePosition(mainElectStatus.Position)
	if err != nil {
		return fmt.Errorf("cannot decode main elect position %v: %v", mainElectStatus.Position, err)
	}
	for alias, status := range statusMap {
		if alias == mainElectTabletAliasStr {
			continue
		}
		pos, err := mysql.DecodePosition(status.Position)
		if err != nil {
			return fmt.Errorf("cannot decode subordinate %v position %v: %v", alias, status.Position, err)
		}
		if !mainElectPos.AtLeast(pos) {
			return fmt.Errorf("tablet %v is more advanced than main elect tablet %v: %v > %v", alias, mainElectTabletAliasStr, status.Position, mainElectStatus)
		}
	}

	// Promote the mainElect
	wr.logger.Infof("promote subordinate %v", topoproto.TabletAliasString(mainElectTabletAlias))
	event.DispatchUpdate(ev, "promoting subordinate")
	rp, err := wr.tmc.PromoteSubordinate(ctx, mainElectTabletInfo.Tablet)
	if err != nil {
		return fmt.Errorf("main-elect tablet %v failed to be upgraded to main: %v", topoproto.TabletAliasString(mainElectTabletAlias), err)
	}

	// Check we stil have the topology lock.
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return fmt.Errorf("lost topology lock, aborting: %v", err)
	}

	// Create a cancelable context for the following RPCs.
	// If error conditions happen, we can cancel all outgoing RPCs.
	replCtx, replCancel := context.WithCancel(ctx)
	defer replCancel()

	// Reset replication on all subordinates to point to the new main, and
	// insert test row in the new main.
	// Go through all the tablets:
	// - new main: populate the reparent journal
	// - everybody else: reparent to new main, wait for row
	event.DispatchUpdate(ev, "reparenting all tablets")
	now := time.Now().UnixNano()
	wgMain := sync.WaitGroup{}
	wgSubordinates := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	var mainErr error
	for alias, tabletInfo := range tabletMap {
		if alias == mainElectTabletAliasStr {
			wgMain.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgMain.Done()
				wr.logger.Infof("populating reparent journal on new main %v", alias)
				mainErr = wr.tmc.PopulateReparentJournal(replCtx, tabletInfo.Tablet, now, emergencyReparentShardOperation, mainElectTabletAlias, rp)
			}(alias, tabletInfo)
		} else {
			wgSubordinates.Add(1)
			go func(alias string, tabletInfo *topo.TabletInfo) {
				defer wgSubordinates.Done()
				wr.logger.Infof("setting new main on subordinate %v", alias)
				forceStartSubordinate := false
				if status, ok := statusMap[alias]; ok {
					forceStartSubordinate = status.SubordinateIoRunning || status.SubordinateSqlRunning
				}
				if err := wr.tmc.SetMain(replCtx, tabletInfo.Tablet, mainElectTabletAlias, now, forceStartSubordinate); err != nil {
					rec.RecordError(fmt.Errorf("tablet %v SetMain failed: %v", alias, err))
				}
			}(alias, tabletInfo)
		}
	}

	// After the main is done, we can update the shard record
	// (note with semi-sync, it also means at least one subordinate is done)
	wgMain.Wait()
	if mainErr != nil {
		// The main failed, there is no way the
		// subordinates will work.  So we cancel them all.
		wr.logger.Warningf("main failed to PopulateReparentJournal, canceling subordinates")
		replCancel()
		wgSubordinates.Wait()
		return fmt.Errorf("failed to PopulateReparentJournal on main: %v", mainErr)
	}
	wr.logger.Infof("updating shard record with new main %v", topoproto.TabletAliasString(mainElectTabletAlias))
	if _, err := wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.MainAlias = mainElectTabletAlias
		return nil
	}); err != nil {
		wgSubordinates.Wait()
		return fmt.Errorf("failed to update shard main record: %v", err)
	}

	// Wait for the subordinates to complete. If some of them fail, we
	// will rebuild the shard serving graph anyway
	wgSubordinates.Wait()
	if err := rec.Error(); err != nil {
		wr.Logger().Errorf2(err, "some subordinates failed to reparent")
		return err
	}

	return nil
}
