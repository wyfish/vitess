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

package test

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
)

func waitForMainID(t *testing.T, mp topo.MainParticipation, expected string) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		main, err := mp.GetCurrentMainID(context.Background())
		if err != nil {
			t.Fatalf("GetCurrentMainID failed: %v", err)
		}

		if main == expected {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("GetCurrentMainID timed out with %v, expected %v", main, expected)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// checkElection runs the tests on the MainParticipation part of the
// topo.Conn API.
func checkElection(t *testing.T, ts *topo.Server) {
	conn, err := ts.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		t.Fatalf("ConnForCell(global) failed: %v", err)
	}
	name := "testmp"

	// create a new MainParticipation
	id1 := "id1"
	mp1, err := conn.NewMainParticipation(name, id1)
	if err != nil {
		t.Fatalf("cannot create mp1: %v", err)
	}

	// no main yet, check name
	waitForMainID(t, mp1, "")

	// wait for id1 to be the main
	ctx1, err := mp1.WaitForMainship()
	if err != nil {
		t.Fatalf("mp1 cannot become main: %v", err)
	}

	// A lot of implementations use a toplevel directory for their elections.
	// Make sure it is marked as 'Ephemeral'.
	entries, err := conn.ListDir(context.Background(), "/", true /*full*/)
	if err != nil {
		t.Fatalf("ListDir(/) failed: %v", err)
	}
	for _, e := range entries {
		if e.Name != topo.CellsPath {
			if !e.Ephemeral {
				t.Errorf("toplevel directory that is not ephemeral: %v", e)
			}
		}
	}

	// get the current main name, better be id1
	waitForMainID(t, mp1, id1)

	// create a second MainParticipation on same name
	id2 := "id2"
	mp2, err := conn.NewMainParticipation(name, id2)
	if err != nil {
		t.Fatalf("cannot create mp2: %v", err)
	}

	// wait until mp2 gets to be the main in the background
	mp2IsMain := make(chan error)
	var mp2Context context.Context
	go func() {
		var err error
		mp2Context, err = mp2.WaitForMainship()
		mp2IsMain <- err
	}()

	// ask mp2 for main name, should get id1
	waitForMainID(t, mp2, id1)

	// stop mp1
	mp1.Stop()

	// this should have closed ctx1 as soon as possible,
	// so 5s should be enough in tests. This will be used during lameduck
	// when the server exits, so we can't wait too long anyway.
	select {
	case <-ctx1.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("shutting down mp1 didn't close ctx1 in time")
	}

	// now mp2 should be main
	err = <-mp2IsMain
	if err != nil {
		t.Fatalf("mp2 awoke with error: %v", err)
	}

	// ask mp2 for main name, should get id2
	waitForMainID(t, mp2, id2)

	// stop mp2, we're done
	mp2.Stop()

	// mp2Context should then close.
	select {
	case <-mp2Context.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("shutting down mp2 didn't close mp2Context in time")
	}

	// At this point, we should be able to call WaitForMainship
	// again, and it should return topo.ErrInterrupted.  Testing
	// this here as this is what the vtctld workflow manager loop
	// does, for instance. There is a go routine that runs
	// WaitForMainship and needs to exit cleanly at the end.
	_, err = mp2.WaitForMainship()
	if !topo.IsErrType(err, topo.Interrupted) {
		t.Errorf("wrong error returned by WaitForMainship, got %v expected %v", err, topo.NewError(topo.Interrupted, ""))
	}
}
