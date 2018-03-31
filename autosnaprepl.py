#!/usr/bin/env python3

# This script is intented to be run automatically (such as via crontab) as well as manually.
# Most of the options are not necessary for an automatic run but may be useful in a manual run.
#
# Config file (/etc/zfs-auto.json) of the form:
# {
#   "snapshots":{
#     "home": {
#       "enabled":true,
#       "dataset":"tank/home",
#       "frequency":"1h",
#       "duration":"2w",
#       "remoteCmd":["ssh", "remotehost"]
#     }
#   },
#   "replications":{
#     "home": {
#       "enabled":true,
#       "dataset":"tank/home",
#       "destination":"woody/backup",
#       "srcCmd":["ssh", "remotehosta"],
#       "dstCmd":["ssh", "remotehostb"],
#       "sendLargeBlocks":true,
#       "sendCompressed":true
#     }
#   }
# }
# 
# In both the snapshots and replications, the "enabled" key is optional (assumed true)
# The remote commands are necessary if the pool in question (snapshot/src/dst) is on a remote system
# otherwise it may be ommitted.
# The name of the snapshot or replication is the name of the task.  There is no connection between 
# a snapshot and replication task of the same name
# 
# Snapshots:
# The "dataset" is the dataset you wish to snapshot; snapshots are recursive
# The "frequency" is how often you want to take a snapshot.  It is a number and a unit where the unit is
# [h]ours, [d]ays, [w]eeks, or [y]ears
# The "duration" is how long the snapshot should last.  Also a number and unit like "frequency"
# 
# Replications:
# The "dataset" filesystem is replicated to the "destination".  Note, a "dataset" of "tank/home"
# and a "destination" of "woody/backup" will replicate "tank/home" into "woody/backup/home"
# Replications are recusive
# Both the "sendLargeBlocks" and "sendCompressed" keys are optional (assumed false) and control the use of
# the "-L" and "-c" flags on zfs send.


from zfs_functions import *

import argparse
import fcntl
import json
import os
import sys

class ConfigObject(object):
  def __init__(self, name, config):
    super(ConfigObject, self).__init__()
    self.config = config
    self.jobId = name
    self.error = self.validate()

  def validate(self):
    self.enabled = True
    if 'enabled' in self.config:
      self.enabled = self.config['enabled']
      if not isinstance(self.enabled, bool):
        return '"enabled" must be a bool type'

    if not 'dataset' in self.config:
      return self.getRequired()

    self.dataset = self.config['dataset']
    if not isinstance(self.dataset, str):
      return '"dataset" must be a string type'

    return ''


class SnapshotConfig(ConfigObject):
  def __init__(self, name, config):
    super(SnapshotConfig, self).__init__(name, config)

  def validate(self):
    error = super(SnapshotConfig, self).validate()
    if len(error):
      return error

    if not 'frequency' in self.config or not 'duration' in self.config:
      return self.getRequired()
    
    frequencyStr = self.config['frequency']
    if not isinstance(frequencyStr, str):
      return '"frequency" must be a string type'

    try:
      self.frequency = TimeSnapshots.Get_time_delta(frequencyStr)
    except ValueError:
      return 'Invalid frequency ' + frequencyStr

    self.durationStr = self.config['duration']
    if not isinstance(self.durationStr, str):
      return '"duration" must be a string type'

    try:
      self.duration = TimeSnapshots.Get_time_delta(self.durationStr)
    except ValueError:
      return 'Invalid duration ' + self.durationStr

    if 'remoteCmd' in self.config:
      self.remoteCmd = self.config['remoteCmd']
      if not isinstance(self.remoteCmd, list):
        return '"remoteCmd" must be a list of strings if present'
      for cmd in self.remoteCmd:
        if not isinstance(cmd, str):
          return '"remoteCmd" must be a list of strings if present'
    else:
      self.remoteCmd = []

    return ''

  def getRequired(self):
    return '\"dataset\", \"frequency\", and \"duration\" are all required in a snapshot job'

class ReplicationConfig(ConfigObject):
  def __init__(self, name, config):
    super(ReplicationConfig, self).__init__(name, config)

  def validate(self):
    error = super(ReplicationConfig, self).validate()
    if len(error):
      return error

    if not 'destination' in self.config:
      return self.getRequired()
    self.destination = self.config['destination']
    if not isinstance(self.destination, str):
      return '"destination" must be a string type'

    if 'srcCmd' in self.config:
      self.srcCmd = self.config['srcCmd']
      if not isinstance(self.srcCmd, list):
        return '"srcCmd" must be a list of strings if present'
      for cmd in self.srcCmd:
        if not isinstance(cmd, str):
          return '"srcCmd" must be a list of strings if present'
    else:
      self.srcCmd = []

    if 'dstCmd' in self.config:
      self.dstCmd = self.config['dstCmd']
      if not isinstance(self.dstCmd, list):
        return '"dstCmd" must be a list of strings if present'
      for cmd in self.dstCmd:
        if not isinstance(cmd, str):
          return '"dstCmd" must be a list of strings if present'
    else:
      self.dstCmd = []

    if 'sendLargeBlocks' in self.config:
      self.sendLargeBlocks = self.config['sendLargeBlocks']
      if not isinstance(self.sendLargeBlocks, bool):
        return '"sendLargeBlocks" must be a boolean type if present'
    else:
      self.sendLargeBlocks = False

    if 'sendCompressed' in self.config:
      self.sendCompressed = self.config['sendCompressed']
      if not isinstance(self.sendCompressed, bool):
        return '"sendCompressed" must be a boolean type if present'
    else:
      self.sendCompressed = False

    return ''
  
  def getRequired(self):
    return '\"dataset\", \"jobId\", and \"destination\" are all required in a replication job'

class SnapshotAndRepl():
  def __init__(self, statePath, configPath):
    self.statePath = statePath
    stateDir = os.path.dirname(statePath)
    if not os.path.exists(stateDir):
      os.mkdir(stateDir)
    self.configPath = configPath
    self.lock()
    if os.stat(self.statePath).st_size == 0:
      state = {
        'lastSnapshotMade': {},
        'lastSnapshotPerJob': {},
        'lastSnapshotReplicatedPerJob': {}
      }
      self.saveStateAndUnlock(state)
    else:
      self.unlock()
    
    self.locks = {}
    self.verbose = False
    self.dryRun = False
    self.printOutput = False
    self.readConfig()

  def lock(self):
    """ Lock the state file """
    try:
      self.fd = open(self.statePath, 'x+')
    except FileExistsError:
      self.fd = open(self.statePath, 'r+')
    fcntl.flock(self.fd, fcntl.LOCK_EX)

  def unlock(self):
    """ Unlock the state file """
    fcntl.flock(self.fd, fcntl.LOCK_UN)
    self.fd.close()
    self.fd = None

  def getState(self):
    """Get the state, lock to read and then immediately unlock"""
    self.lock()
    self.fd.seek(0)
    state = json.load(self.fd)
    self.unlock()
    return state

  def lockAndGetState(self):
    """ Get the current state with the file locked """
    self.lock()
    self.fd.seek(0)
    return json.load(self.fd)

  def saveStateAndUnlock(self, state):
    """ Save the current state to the file and unlock"""
    self.fd.seek(0)
    json.dump(state, self.fd)
    self.fd.truncate()
    self.unlock()

  def lockForType(self, type):
    """ Set this process as the one doing a particular process, returns False if another proc is doing it"""
    while True:
      try:
        path = '/tmp/autosnaprepl-' + type
        fd = os.open(path, os.O_WRONLY | os.O_CREAT)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fst = os.fstat(fd)

        # Make sure the file didn't get removed (and possibly replaced) since we locked it
        try:
          st = os.stat(path)

          if fst.st_ino == st.st_ino:
            self.locks[type] = fd
            return True

        except OSError as e:
          pass

        os.close(fd)

      except IOError as e:
        return False

  def unlockForType(self, type):
    """ Release this process as the one doing a particular process """
    fd = self.locks[type]
    os.remove('/tmp/autosnaprepl-' + type)
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
    self.locks.pop(type, None)
  
  def lockForSnapshot(self):
    """ Set this process as the one taking snapshots, returns False if another proc is doing it """
    return self.lockForType('snapshot')

  def unlockForSnapshot(self):
    """ Release this process as the one taking snapshots """
    self.unlockForType('snapshot')

  def lockForReplication(self):
    """ Set this process as the one replicating, returns False if another proc is doing it """
    return self.lockForType('replication')

  def unlockForReplication(self):
    """ Release this process as the one replicating """
    self.unlockForType('replication')

  def saveLastSnapshot(self, dataset, jobId, snapshotName):
    """ Save the last snapshot made for a particular dataset """
    state = self.lockAndGetState()
    state['lastSnapshotMade'][dataset] = snapshotName
    state['lastSnapshotPerJob'][jobId] = snapshotName
    self.saveStateAndUnlock(state)

  def saveLastReplication(self, jobId, snapshotName):
    """ Save the last snapshot replicated for a particular dataset """
    state = self.lockAndGetState()
    state['lastSnapshotReplicatedPerJob'][jobId] = snapshotName
    self.saveStateAndUnlock(state)

  def readConfig(self):
    """ Read configuration file """
    with open(self.configPath) as data:
      self.config = json.load(data)

    if not isinstance(self.config, dict):
      print('Configuration must be a dict type')
      sys.exit(1)

    if not 'snapshots' in self.config:
      print('Configuration missing snapshot configuration')
      sys.exit(1)

    if not isinstance(self.config['snapshots'], dict):
      print('Configuration for snapshots must be a dict type')
      sys.exit(1)
    
    self.snapshots = []
    for name, snapshotConfig in self.config['snapshots'].items():
      snapshot = SnapshotConfig(name, snapshotConfig)
      if len(snapshot.error):
        print("In snapshot config \"{name}\": {error}".format(name=name, error=snapshot.error))
        sys.exit(1)
      
      self.snapshots.append(snapshot)
    
    if not 'replications' in self.config:
      print('Configuration missing replication configuration')
      sys.exit(1)

    if not isinstance(self.config['replications'], dict):
      print('Configuration for replications must be a dict type')
      sys.exit(1)
    
    self.replications = []
    for name, replicationConfig in self.config['replications'].items():
      replication = ReplicationConfig(name, replicationConfig)
      if len(replication.error):
        print("In replication config \"{name}\": {error}".format(name=name, error=replication.error))
        sys.exit(1)

      self.replications.append(replication)

  def getLastSnapshot(self, snapshotJobId):
    """ Get the last snapshot made for a dataset """
    state = self.getState()
    
    if snapshotJobId in state['lastSnapshotPerJob']:
      return state['lastSnapshotPerJob'][snapshotJobId]

    return None

  def runSnapshots(self):
    """ Make snapshots """
    if not self.lockForSnapshot():
      return False

    now = datetime.datetime.today()
    snapshotMade = False
    for snapshotJob in self.snapshots:
      jobId = snapshotJob.jobId
      if self.verbose:
        print("Running snapshot: {job}".format(job=jobId))
      if not snapshotJob.enabled:
        if self.verbose:
          print("Not enabled; skipping")
        continue

      dataset = snapshotJob.dataset
      lastSnapshot = self.getLastSnapshot(jobId)
      if self.verbose:
        name = lastSnapshot
        if name == None:
          name = "None"
        print("Checking if snapshot needed with last: {name}".format(name=name))
      required = False
      if lastSnapshot == None:
        required = True
      elif TimeSnapshots.Get_snapshot_time(lastSnapshot) + snapshotJob.frequency <= now:
        required = True

      if required:
        poolName = dataset.split("/")[0]
        try:
          pool=ZFS_pool(pool=poolName, remote_cmd=snapshotJob.remoteCmd, verbose=self.verbose)
        except subprocess.CalledProcessError:
          print("Cannot get pool {cmd}:{name} to make snapshots.".format(cmd=" ".join(snapshotJob.remoteCmd), name=poolName))
          continue
        datasetLen=dataset.rfind('/')+1
        for fs in pool.get_zfs_filesystems(fs_filter=dataset):
          if self.verbose:
            print("Making timed snapshot for: {fs}".format(fs=fs))
          fs=ZFS_fs(fs=fs, pool=pool, verbose=self.verbose, dry_run=self.dryRun)
          timed=TimeSnapshots(fs=fs)
          snapshot = timed.take_snapshot(durationStr=snapshotJob.durationStr)
          snapshotName = snapshot.split('@')[1]
          self.saveLastSnapshot(dataset, jobId, snapshotName)
          snapshotMade = True
          timed.expire_snapshots()

    self.unlockForSnapshot()
    return snapshotMade

  def needReplication(self, state):
    for replicationJob in self.replications:
      if not replicationJob.enabled:
        continue

      jobId = replicationJob.jobId
      if not jobId in state['lastSnapshotReplicatedPerJob']:
        if self.verbose:
          print("No replications done; require replication")
        return True

      dataset = replicationJob.dataset
      if dataset in state['lastSnapshotMade'] and state['lastSnapshotMade'][dataset] != state['lastSnapshotReplicatedPerJob'][jobId]:
        if self.verbose:
          print("Last snapshot made is not last replicated; require replication")
        return True

    if self.verbose:
      print("All replications are already complete")

    return False

  def startReplicationAndLock(self, force):

    """ Starts the replication process if needed; return False if not """
    if not self.lockForReplication():
      if self.verbose:
        print("Replication already running")
      return False

    if force:
      return True

    state = self.lockAndGetState()
    needed = self.needReplication(state)
    
    if not needed:
      self.unlockForReplication()

    self.unlock()

    return needed

  def getSnapshotNeedingReplication(self, replicationJob, force):
    """ Determine if the given replication is needed """
    state = self.getState()
    
    dataset = replicationJob.dataset
    jobId = replicationJob.jobId
    if dataset in state['lastSnapshotMade']:
      lastSnapshot = state['lastSnapshotMade'][dataset]
      perJobSnaps = state['lastSnapshotReplicatedPerJob']
      if force or not jobId in perJobSnaps or lastSnapshot != perJobSnaps[jobId]:
        return lastSnapshot

    return None

  def runReplication(self, force):
    """ Start replication """
    while self.startReplicationAndLock(force):
      for replicationJob in self.replications:
        jobId = replicationJob.jobId
        if self.verbose:
          print("Running replication: {job}".format(job=jobId))
        if not replicationJob.enabled:
          if self.verbose:
            print("Not enabled; skipping")
          continue

        snapshotNeedingReplication = self.getSnapshotNeedingReplication(replicationJob, force)
        if snapshotNeedingReplication == None:
          if self.verbose:
            print("Replication already complete")
          continue

        if self.verbose:
          print("Snapshot {snapshot} needs replication.".format(snapshot=snapshotNeedingReplication))

        srcDataset = replicationJob.dataset
        srcPool = srcDataset.split('/')[0]
        dstDataset = replicationJob.destination
        dstPool = dstDataset.split('/')[0]
        try:
          src=ZFS_pool(pool=srcPool, remote_cmd=replicationJob.srcCmd, verbose=self.verbose)
          dst=ZFS_pool(pool=dstPool, remote_cmd=replicationJob.dstCmd, verbose=self.verbose)
        except subprocess.CalledProcessError:
          print("Could not open source/destination: {srcCmd}:{srcPool}, {dstCmd}:{dstPool}".format(srcCmd=" ".join(srcCmd), srcPool=srcPool, dstCmd=" ".join(dstCmd), dstPool=dstPool))
          continue
        srcPrefixLen=srcDataset.rfind('/')+1

        failure = False
        for fs in src.get_zfs_filesystems(fs_filter=srcDataset):
          if self.verbose:
            print("Replicating for: {fs}".format(fs=fs))
          srcFS=ZFS_fs(fs=fs, pool=src, verbose=self.verbose, dry_run=self.dryRun)
          dstFS=ZFS_fs(fs=dstDataset+"/"+fs[srcPrefixLen:], pool=dst, verbose=self.verbose, dry_run=self.dryRun)

          srcFS.send_large_blocks = replicationJob.sendLargeBlocks
          self.send_compressed = replicationJob.sendCompressed

          if not srcFS.sync_without_snap(dst_fs=dstFS, print_output=self.printOutput):
            print("sync failure for {fs} from {src} to {dst} at {dataset}".format(fs=fs, src=src, dst=dst, dataset=dstDataset))
            failure = True
            break
          srcFS.remove_deleted_snapshots(dst_fs=dstFS)

        if not failure:
          self.saveLastReplication(jobId, snapshotNeedingReplication)

      force=False
      self.unlockForReplication()


if __name__ == '__main__':
  parser=argparse.ArgumentParser(description="Automatically takes snapshots and replicate.")
  parser.add_argument("--config", help="The path of the config file")
  parser.add_argument("--force-replication", help="Force replication even if last replication has already synced snapshots.", action="store_true")
  parser.add_argument("--dry-run", help="Just display what would be done. Notice that since no snapshots will be taken, less will be marked for replication. ", action="store_true")
  parser.add_argument("--verbose", help="Display what is being done", action="store_true")
  parser.add_argument("--print-output", help="Print the output of zfs receive", action="store_true")

  args=parser.parse_args()

  if args.config != None:
    config = args.config
  else:
    config = '/etc/zfs-auto.json'
  
  proc = SnapshotAndRepl('/var/lib/zfs-auto/state', config)
  proc.dryRun = args.dry_run
  proc.verbose = args.verbose
  proc.printOutput = args.print_output

  proc.runSnapshots()
  proc.runReplication(args.force_replication)