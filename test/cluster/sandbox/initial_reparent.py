#!/usr/bin/env python

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Performs the first reparent on every shard of a keyspace."""

import json
import logging
import optparse
import sys
import time
from vtproto import topodata_pb2
from vttest import sharding_utils
import vtctl_sandbox


def is_main(tablet, namespace):
  tablet_info = (
      vtctl_sandbox.execute_vtctl_command(
          ['GetTablet', tablet], namespace=namespace))
  if json.loads(tablet_info[0])['type'] == topodata_pb2.MASTER:
    return True


def initial_reparent(keyspace, main_cell, num_shards, namespace, timeout_s):
  """Performs the first reparent."""
  successfully_reparented = []
  main_tablets = {}
  start_time = time.time()
  logging.info('Finding tablets to reparent to.')
  while len(main_tablets) < num_shards:
    if time.time() - start_time > timeout_s:
      logging.error('Timed out waiting to find a replica tablet')
      return 1

    for shard_name in sharding_utils.get_shard_names(num_shards):
      if shard_name in main_tablets:
        continue
      tablets = vtctl_sandbox.execute_vtctl_command(
          ['ListShardTablets', '%s/%s' % (keyspace, shard_name)],
          namespace=namespace)[0].split('\n')
      tablets = [x.split(' ') for x in tablets if x]
      potential_mains = [
          x[0] for x in tablets if x[3] == 'replica'
          and x[0].split('-')[0] == main_cell]
      if potential_mains:
        main_tablets[shard_name] = potential_mains[0]
        logging.info(
            '%s selected for shard %s', potential_mains[0], shard_name)

  while time.time() - start_time < timeout_s:
    for shard_name in sharding_utils.get_shard_names(num_shards):
      main_tablet_id = main_tablets[shard_name]
      if is_main(main_tablet_id, namespace):
        logging.info('Tablet %s is the main of %s/%s.',
                     main_tablet_id, keyspace, shard_name)
        successfully_reparented.append(shard_name)
      if shard_name in successfully_reparented:
        continue
      logging.info('Setting tablet %s as main for %s/%s.',
                   main_tablet_id, keyspace, shard_name)
      vtctl_sandbox.execute_vtctl_command(
          ['InitShardMain', '-force', '%s/%s' % (keyspace, shard_name),
           main_tablet_id], namespace=namespace, timeout_s=5)
    if len(successfully_reparented) == num_shards:
      logging.info('Done with initial reparent.')
      return 0

  logging.error('Timed out waiting for initial reparent.')
  return 1


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] [test_names]')
  parser.add_option('-n', '--namespace', help='Kubernetes namespace',
                    default='vitess')
  parser.add_option('-k', '--keyspace', help='Keyspace name',
                    default='test_keyspace')
  parser.add_option('-m', '--main_cell', help='Main cell')
  parser.add_option('-s', '--shard_count', help='Number of shards', default=2,
                    type=int)
  parser.add_option('-t', '--timeout', help='Reparent timeout (s)', default=300,
                    type=int)
  logging.getLogger().setLevel(logging.INFO)

  options, _ = parser.parse_args()
  sys.exit(initial_reparent(options.keyspace, options.main_cell,
                            options.shard_count, options.namespace,
                            options.timeout))


if __name__ == '__main__':
  main()
