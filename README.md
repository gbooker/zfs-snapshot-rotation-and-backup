Several python scripts for automation of snapshots and replications

# autosnaprepl.py

The script offering the most automation.  Intended to be run from cron at a rapid interval (such as every minute), or manually, or both.  User defines a set of jobs for both taking snapshots at specific intervals and replication of snapshots to other pools, local or remote.  Can additionally be run manually to watch the progress.

Intended to be used for the automatic creation of snapshots and backup to another pool.  For example, it can be configured to take a snapshot every hour that lasts for 2 weeks, and synchronize these snapshots to a remote system.

See the top of [the file itself](autosnaprepl.py) for more information.

# snap.py

Creates a snapshot on a specified file system intended to last a specified duration.  Additionally, destroys snapshots which have reached their expiry.  Can be used recursively.

# sync.py

Sends snapshots from a source filesystem to a destination file system.  It queries both the source and destination to determine the start and end snapshot range to send and will then destroy snapshots on the destination which are no longer present on the source.  Can be used recursively.

### Original Description

Some python scripts to manage rotating zfs snapshots via cron/anacron.

Includes customizable numbers of snapshots to keep for
daily/weekly/monthly or arbitrary interval snapshots.

Also includes functions to synchronize snapshots (recursively) over ssh.

This started out as a collection of shell scripts (see git history)
but I rewrote it completely in python for better functionality and
better scriptability.
 