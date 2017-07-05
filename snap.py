#!/usr/bin/env python2

import argparse
import sys

from zfs_functions import *

if __name__ == '__main__':
  parser=argparse.ArgumentParser(description="Make timed snapshots and delete expired snapshots.")
  parser.add_argument("src", help="The source zfs filesystem to snapshot.")
  parser.add_argument("duration", help="The duration of the snapshot's life time, eg 2w.  Units can be [h]ours, [d]ays, [w]eeks, and [y]ears.")
  parser.add_argument("-s", metavar="cmd", help="The source fs command (ssh remotehost).")
  parser.add_argument("--dry-run", help="Just display what would be done.", action="store_true")
  parser.add_argument("--verbose", help="Display what is being done.", action="store_true")

  args=parser.parse_args()
  # print(args)
  
  pool = args.src.split("/")[0]
  prefix = args.src
  
  durationStr = args.duration
  duration=int(durationStr[:1])
  unit=durationStr[len(durationStr)-1]
  if not unit in ["h", "d", "w", "y"]:
    print ("Invalid duration "+durationStr)
    sys.exit()

  cmd = args.s if args.s != None else ""

  try:
    src=ZFS_pool(pool=pool, remote_cmd=cmd, verbose=args.verbose)
  except subprocess.CalledProcessError:
    sys.exit()

  prefix_len=prefix.rfind('/')+1
  for fs in src.get_zfs_filesystems(fs_filter=prefix):
    fs=ZFS_fs(fs=fs, pool=src, verbose=args.verbose, dry_run=args.dry_run)
    timed=TimeSnapshots(fs=fs)
    timed.take_snapshot(duration=duration,unit=unit)
    timed.expire_snapshots()
