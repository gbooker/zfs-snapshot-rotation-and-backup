#!/usr/bin/env python2

import argparse
import shlex
import sys

from zfs_functions import *

if __name__ == '__main__':
  parser=argparse.ArgumentParser(description="Sync snapshots from one FS to another and remove snapshots on destination not present on source.")
  parser.add_argument("src", help="The source zfs filesystem to sync to destination.")
  parser.add_argument("dst", help="The destination zfs filesystem to sync from source.  Use a trailing / to indicate that source name should be appended to destination.  \"sync.py a/src b/dst/\" will sync a/src to b/dst/src where as \"sync.py a/src b/dst\" will sync a/src to b/dst.")
  parser.add_argument("-s", metavar="src_cmd", help="The source fs command (ssh remotehost).")
  parser.add_argument("-d", metavar="dst_cmd", help="The destination fs command (ssh remotehost).")
  parser.add_argument("--dry-run", help="Just display what would be done. Notice that since no snapshots will be transfered, less will be marked for theoretical destruction.", action="store_true")
  parser.add_argument("--verbose", help="Display what is being done.", action="store_true")
  parser.add_argument("--print-output", help="Print the output of zfs receive.", action="store_true")

  args=parser.parse_args()
  # print(args)
  
  src_pool = args.src.split("/")[0]
  src_prefix = args.src
  
  dst_pool = args.dst.split("/")[0]
  dst_prefix = args.dst
  
  src_cmd = shlex.split(args.s) if args.s != None else []
  dst_cmd = shlex.split(args.d) if args.d != None else []

  try:
    src=ZFS_pool(pool=src_pool, remote_cmd=src_cmd, verbose=args.verbose)
    dest=ZFS_pool(pool=dst_pool, remote_cmd=dst_cmd, verbose=args.verbose)
  except subprocess.CalledProcessError:
    sys.exit()

  if dst_prefix[len(dst_prefix) - 1] == '/':
    src_prefix_len=src_prefix.rfind('/')+1
  else:
    src_prefix_len=len(src_prefix)

  for fs in src.get_zfs_filesystems(fs_filter=src_prefix):
    src_fs=ZFS_fs(fs=fs, pool=src, verbose=args.verbose, dry_run=args.dry_run)
    dst_fs=ZFS_fs(fs=dst_prefix+fs[src_prefix_len:], pool=dest, verbose=args.verbose, dry_run=args.dry_run)
    if not src_fs.sync_without_snap(dst_fs=dst_fs,print_output=args.print_output):
      print ("sync failure for "+fs)
      sys.exit()
    src_fs.remove_deleted_snapshots(dst_fs=dst_fs)
