'''
Created on 4 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import subprocess
import datetime
import signal
import time
import pprint
pp = pprint.PrettyPrinter(indent=4)

class ZFS_iterator:
	verbose=False
	dry_run=False
	i = 0

	def __init__(self,pool):
		self.pool=pool
		self.i = 0

	def __iter__(self):
			return self

	def next(self):
		if self.i < len(self.pool.zfs_filesystems):
			i = self.i
			self.i += 1
			origin=self.pool.get_origin(fs=self.pool.zfs_filesystems[i])
			if origin != "-":
				origin=origin.split('@')[0]
				a, b = self.pool.zfs_filesystems.index(self.pool.zfs_filesystems[i]),\
					self.pool.zfs_filesystems.index(origin)
				if a < b:
					self.pool.zfs_filesystems[b], self.pool.zfs_filesystems[a] = \
						self.pool.zfs_filesystems[a], self.pool.zfs_filesystems[b]
			return self.pool.zfs_filesystems[i]
		else:
			raise StopIteration()

	def __iter__(self):
			return self

	def next(self):
		if self.i < len(self.pool.zfs_filesystems):
			i = self.i
			self.i += 1
			return self.pool.zfs_filesystems[i]
		else:
			raise StopIteration()

class Pending_Command:
	def __init__(self, pool, args=[]):
		self.pool = pool
		self.args = args

	def __str__(self):
		return "cmd["+" ".join(self.pool.remote_cmd)+":"+" ".join(self.args)+"]"

class ZFS_pool:
	verbose=False
	dry_run=False
	destructive=False
	def __str__(self):
		return "pool["+" ".join(self.remote_cmd)+"]<"+self.pool+">"

	def __init__(self,pool,remote_cmd=[],verbose=False,dry_run=False,destructive=False,veryVerbose=False):
		self.pool=pool
		self.remote_cmd=remote_cmd
		self.verbose=verbose
		self.veryVerbose=veryVerbose
		self.dry_run=dry_run
		self.destructive=destructive
		self.zfs_filesystems=[]
		self.zfs_snapshots=[]
		self.update_zfs_filesystems()
		self.update_zfs_snapshots()

	def prepare_command(self, args):
		args = self.remote_cmd + [arg for arg in args if arg != None]
		if self.veryVerbose:
			print("Going to run: {cmd}".format(cmd=" ".join(args)));
		return args
		
	def command_to_string(self, args):
		return " ".join(self.prepare_command(args))
		
	def remote_exec(self, args):
		return subprocess.check_output(self.prepare_command(args), universal_newlines=True)

	def remote_exec_no_out(self, args):
		return subprocess.check_call(self.prepare_command(args))

	def remote_exec_piped(self, args, input_pipe=None):
		p = subprocess.Popen(self.prepare_command(args), stdin=input_pipe, stdout=subprocess.PIPE)
		if input_pipe:
			# So that previous process receives a SIG_PIPE if this one exits
			input_pipe.close()

		return p

	def update_zfs_snapshots(self, timeout=180):
		with TimeoutObject(timeout):
			waitfor_cmd_to_exit(remote=self.remote_cmd, cmd_line_parts=["zfs","list","-t","snapshot"], sleep=10)
		snapshot_list = self.remote_exec(["zfs", "list", "-o", "name", "-t", "snapshot", "-H", "-r", self.pool]).split("\n")
		self.zfs_snapshots=snapshot_list
		return snapshot_list

	def update_zfs_filesystems(self):
		fs_list = self.remote_exec(["zfs", "list", "-o", "name", "-H", "-r", self.pool]).split("\n")
		fs_list = fs_list[0:-1]
		i=0
		while i < len(fs_list):
			origin=self.get_origin(fs_list[i])
			if origin != "-":
				origin=origin.split('@')[0]
				a, b = fs_list.index(fs_list[i]), fs_list.index(origin)
				if a < b:
					fs_list[b], fs_list[a] = fs_list[a], fs_list[b]
			i += 1
		self.zfs_filesystems=fs_list
		return self.zfs_filesystems

	def get_zfs_snapshots(self,fs="", recursive=False):
		match=fs if recursive else fs+"@"
		for snapshot in self.zfs_snapshots:
			if snapshot.startswith(match):
				yield snapshot

	def get_zfs_snapshots_reversed(self,fs="", recursive=False):
		match=fs if recursive else fs+"@"
		for snapshot in reversed(self.zfs_snapshots):
			if snapshot.startswith(match):
				yield snapshot

	def __iter__(self):
		return ZFS_iterator(self)

	def get_origin(self,fs=None):
		origin = self.remote_exec(["zfs", "get", "-H", "origin", fs]).split("\t")
		return origin[2]

	def get_zfs_filesystems(self, fs_filter=""):
		for fs in self.zfs_filesystems:
			if fs.startswith(fs_filter):
				yield fs

	def sort_for_destruction(self,fs_filter=""):
		zfs_fs=list(self.zfs_filesystems)
		for fs in zfs_fs:
			fs_parts=fs.split("/")
			if len(fs_parts) > 1:
				parent="/".join(fs_parts[0:len(fs_parts)-1])
				parentIdx=zfs_fs.index(parent)
				if parentIdx < zfs_fs.index(fs):
					zfs_fs.remove(fs)
					zfs_fs.insert(parentIdx,fs)
		for fs in zfs_fs:
			origin=self.get_origin(fs=fs)
			if origin != "-":
				origin=origin.split('@')[0]
				zfs_fs.remove(fs)
				originIdx=zfs_fs.index(origin)
				zfs_fs.insert(originIdx,fs)
		for fs in zfs_fs:
			if fs.startswith(fs_filter):
				yield fs

	def delete_missing_fs_from_target(self,target=None, fs_filter="", target_prefix=""):
		verbose_flag=None
		if self.verbose:
			verbose_flag="-v"
		for fs in target.get_zfs_filesystems(fs_filter=target_prefix+fs_filter):
			if fs[len(target_prefix):] not in self.zfs_filesystems:
				command=["zfs", "destroy", "-R", verbose_flag, fs]
				if self.verbose:
					print("running: {cmd}".format(cmd=self.command_to_string(command)))
				if not self.dry_run:
					self.remote_exec_no_out(command)


	def scrub_running():
		zfs_output = self.remote_exec(["zpool", "status", pool.pool])
		return  "scrub in progress" in zfs_output

class ZFS_fs:

	def __init__(self,fs=None,remote_cmd="", pool=None, verbose=False, dry_run=False, destructive=False):
		self.verbose=verbose
		self.dry_run=dry_run
		if fs==None:
			raise ValueError("No filesystem specified")
		else:
			self.fs=fs
		if pool==None:
			self.pool=ZFS_pool(fs.split("/")[0],remote_cmd=remote_cmd)
		else:
			self.pool=pool
		self.destructive=False
		self.send_large_blocks = False
		self.send_compressed = False

	def __str__(self):
		return ZFS_fs.Snapshot_To_Str(self.pool, self.fs)

	@staticmethod
	def Snapshot_To_Str(pool, snapshot_name):
		return str(pool)+" fs:"+snapshot_name

	def get_snapshots(self):
		return self.pool.get_zfs_snapshots(fs=self.fs, recursive=False)

	def get_snapshots_reversed(self):
		return self.pool.get_zfs_snapshots_reversed(fs=self.fs, recursive=False)

	def get_last_snapshot(self):
		list=self.pool.remote_exec(["zfs", "list", "-o", "name", "-t", "snapshot", "-H", "-r", "-d", "1", self.fs]).split("\n")
		if len(list) < 2:
			return None
		return list[-2]

	def get_first_snapshot(self):
		list=self.pool.remote_exec(["zfs", "list", "-o", "name", "-t", "snapshot", "-H", "-r", "-d", "1", self.fs]).split("\n")
		if len(list) < 2:
			return None
		return list[0]

	def get_last_common_snapshot(self,dst_fs=None):
		snapshots = set()
		for snapshot in dst_fs.get_snapshots():
			snapshots.add(snapshot.split('@')[1])
		
		for snapshot in self.get_snapshots_reversed():
			if snapshot.split('@')[1] in snapshots:
				return snapshot
		return None

	def get_missing_snapshot_names(self,dst_fs=None):
		snapshots = set()
		for snapshot in dst_fs.get_snapshots():
			snapshots.add(snapshot.split('@')[1])
		
		for snapshot in self.get_snapshots():
			snapshots.discard(snapshot.split('@')[1])
		return snapshots

	def create_zfs_snapshot(self,prefix="",name=""):
		if len(prefix)==0 and len(name)==0:
			raise ValueError("prefix for snapshot must be defined")
		if len(name)==0:
			snapshot=self.fs+"@"+prefix+"-"+self.timestamp_string()
		else:
			snapshot=self.fs+"@"+name
		snapshot_command=["zfs", "snapshot", snapshot]
		if self.verbose or self.dry_run:
			print("Running: {cmd}".format(cmd=self.pool.command_to_string(snapshot_command)))
		if not self.dry_run:
			self.pool.remote_exec_no_out(snapshot_command)
			self.pool.zfs_snapshots.append(snapshot)
		return snapshot

	def destroy_zfs_snapshot(self,snapshot):
		snapshot_command=["zfs", "destroy", snapshot]
		if self.verbose or self.dry_run:
			print("Running: {cmd}".format(cmd=self.pool.command_to_string(snapshot_command)))
		if not self.dry_run:
			self.pool.remote_exec_no_out(snapshot_command)

	def get_send_cmd(self):
		send = ["zfs", "send"]
		if self.send_large_blocks:
			send.append("-L")
		if self.send_compressed:
			send.append("-c")

		return send

	def estimate_snapshot_size(self,end_snapshot,start_snapshot=None):
		if start_snapshot==None:
			estimate=self.pool.remote_exec(self.get_send_cmd() + ["-nvP", end_snapshot]).split("\n")[-2]
		else:
			estimate=self.pool.remote_exec(self.get_send_cmd() + ["-nvP", "-I", start_snapshot, end_snapshot]).split("\n")[-2]
		size=estimate.split("size	")[1]
		return size

	def transfer_to(self,dst_fs=None,print_output=False):
		if self.verbose:
			print("trying to transfer: {fs} to {dst}.".format(fs=self, dst=dst_fs))
		if dst_fs.fs in dst_fs.pool.zfs_filesystems:
			if self.verbose:
				print("{dst} already exists.".format(dst=dst_fs))
		if dst_fs.destructive or (dst_fs.fs not in dst_fs.pool.zfs_filesystems):
			if self.verbose:
				print("resetting {dst}".format(dst=dst_fs))
			last_src_snapshot=self.get_last_snapshot()
			first_src_snapshot=self.get_first_snapshot()
			if first_src_snapshot == None:
				print("No snapshots to transfer")
				return False
			for snapshot in dst_fs.get_snapshots_reversed():
				dst_fs.destroy_zfs_snapshot(snapshot)
			firstCmds=[Pending_Command(self.pool, self.get_send_cmd() + ["-p", first_src_snapshot])]
			if first_src_snapshot != last_src_snapshot:
				secondCmds=[Pending_Command(self.pool, self.get_send_cmd() + ["-p", "-I", first_src_snapshot, last_src_snapshot])]
			else:
				secondCmds=None
			if print_output:
				size=self.estimate_snapshot_size(first_src_snapshot)
				firstCmds.append(Pending_Command(self.pool, ["pv", "-pterbs", size]))
				if first_src_snapshot != last_src_snapshot:
					size=self.estimate_snapshot_size(last_src_snapshot,start_snapshot=first_src_snapshot)
					secondCmds.append(Pending_Command(self.pool, ["pv", "-pterbs", size]))
			firstCmds.append(Pending_Command(dst_fs.pool, ["zfs", "receive", "-vF", dst_fs.fs]))
			if first_src_snapshot != last_src_snapshot:
				secondCmds.append(Pending_Command(dst_fs.pool, ["zfs", "receive", "-v", dst_fs.fs]))
			commands=[\
				firstCmds,\
				[Pending_Command(dst_fs.pool, ["zfs", "set", "readonly=on", dst_fs.fs])],\
				secondCmds
			]

			for commandSet in commands:
				if commandSet == None:
					continue
				self.run_command_set(commandSet, print_output)
			last_src_snapshot_name = last_src_snapshot.split("@")[1]
			dst_fs.pool.update_zfs_snapshots()
			for snap in dst_fs.get_snapshots():
				if last_src_snapshot_name in snap.split("@")[1]:
					if self.verbose:
						print("Successfully transferred {lastSnapshot}".format(lastSnapshot=last_src_snapshot))
					return True
			if self.dry_run:
				print("No transfer (dry run)")
				return True
			raise Exception ( "sync : "+str(commands)+" failed")
		print("Destructive transfer not enabled; perhaps there is no common snapshot")
		return False

	def run_command_set(self, commandSet, print_output):
		if self.verbose or self.dry_run:
			print("running {cmds}".format(cmds=" | ".join([str(cmd) for cmd in commandSet])))
		if not self.dry_run:
			prev_pipe=None
			for command in commandSet:
				proc=command.pool.remote_exec_piped(command.args, prev_pipe)
				prev_pipe=proc.stdout

			output=proc.communicate()[0]
			if print_output:
				print(output)

	def sync_without_snap(self,dst_fs=None,print_output=False):
		if dst_fs.fs in dst_fs.pool.zfs_filesystems:
			last_common_snapshot=self.get_last_common_snapshot(dst_fs=dst_fs)
		else:
			if self.verbose:
				print("{dst} does not exist.".format(dst=dst_fs))
			last_common_snapshot=None

		if last_common_snapshot != None:
			if self.verbose:
				print("Sync mark found: {snap}".format(snap=ZFS_fs.Snapshot_To_Str(self.pool, last_common_snapshot)))

			sync_mark_snapshot=self.get_last_snapshot();
			if last_common_snapshot==sync_mark_snapshot:
				if self.verbose:
					print("Snapshots are already synced")
				return True

			return self.run_sync(dst_fs=dst_fs,start_snap=last_common_snapshot,
				stop_snap=sync_mark_snapshot,print_output=print_output)

		else:
			return self.transfer_to(dst_fs=dst_fs,print_output=print_output)

	def sync_with(self,dst_fs=None,target_name="",print_output=False):
		if self.verbose:
			print("Syncing {src} to {dst} with target name {name}.".format(src=self, dst=dst_fs, name=target_name))

		self.create_zfs_snapshot(prefix=target_name)
		self.sync_without_snap(dst_fs=dest_fs,print_output=print_output)

	def run_sync(self,dst_fs=None, start_snap=None, stop_snap=None,print_output=False):
		size=self.estimate_snapshot_size(stop_snap,start_snapshot=start_snap)
		sync_commands=[Pending_Command(self.pool, self.get_send_cmd() + ["-p", "-I", start_snap, stop_snap])]
		if print_output:
			sync_commands.append(Pending_Command(self.pool, ["pv", "-pterbs", size]))
		sync_commands.append(Pending_Command(dst_fs.pool, ["zfs", "receive", "-Fv", dst_fs.fs]))
		self.run_command_set(sync_commands, print_output)
		if not self.dry_run:
			dst_fs.pool.update_zfs_snapshots()
			sync_mark=stop_snap.split("@")[1]
			for snap in dst_fs.get_snapshots():
				if snap.split("@")[1]==sync_mark:
					if self.verbose:
						print("Successfully transferred {lastSnapshot}".format(lastSnapshot=stop_snap))
					return True
			raise Exception ( "sync : "+" | ".join([str(cmd) for cmd in commandSet])+" failed")

		return True

	def remove_deleted_snapshots(self,dst_fs=None):
		if self.verbose:
			print("Removing snapshots previously removed from {src} from {dst}.".format(src=self, dst=dst_fs))

		snapshot_names=self.get_missing_snapshot_names(dst_fs=dst_fs)
		if len(snapshot_names) == 0:
			if self.verbose:
				print("No snapshots to remove")
			return True

		for snapshot_name in sorted(snapshot_names):
			if not dst_fs.destroy_snapshot(snap_to_remove=dst_fs.fs+"@"+snapshot_name):
				return False

		return True

	def rollback(self,snapshot):
		rollback=["zfs", "rollback", "-r", self.fs+"@"+snapshot]
		if self.dry_run==True:
			print(self.pool.command_to_string(rollback))
			return True
		else:
			if self.verbose:
				print("Running rollback: {cmd}".format(cmd=self.pool.command_to_string(rollback)))
			self.pool.remote_exec_no_out(rollback)

	def clean_snapshots(self,prefix="", number_to_keep=None):
		if self.verbose == True:
			print("clean_snapshots: {fs}".format(fs=self))
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			snapshot_parts=snapshot.split("@")
			if snapshot_parts[1].startswith(prefix):
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove=snap_to_remove)

	def clean_other_snapshots(self,prefixes_to_ignore=[], number_to_keep=None):
		if self.verbose == True:
			print("clean_other_snapshots: {fs}".format(fs=self))
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			skip=False
			snapshot_parts=snapshot.split("@")
			for prefix in prefixes_to_ignore:
				if snapshot_parts[1].startswith(prefix):
					skip=True
					break
			if skip==False:
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove=snap_to_remove)

	def destroy_snapshot(self,snap_to_remove):
		command=["zfs", "destroy", self.verbose_switch(), snap_to_remove]
		if self.verbose or self.dry_run:
			print(self.pool.command_to_string(command))
		if not self.dry_run:
			try:
				self.pool.remote_exec_no_out(command)
				self.pool.zfs_snapshots.remove(snap_to_remove)
				return True
			except subprocess.CalledProcessError as e:
				print(e)
				return False

	def timestamp_string(self):
		return datetime.datetime.today().strftime("%F--%H-%M-%S")

	def verbose_switch(self):
		if self.verbose==True:
			return "-v"
		else:
			return None

class TimeSnapshots:

	def __init__(self, fs=None):
		if fs==None:
			raise ValueError("No ZFS filesystem specified")
		else:
			self.fs=fs

	def take_snapshot(self,duration=1,unit="h",durationStr=None):
		if durationStr == None:
			if not unit in ["h", "d", "w", "y"]:
				raise ValueError("Only units of 'h', 'd', 'w', and 'y' are allowed")
			if duration < 1:
				raise ValueError("Duration must be strictly positive")
			durationStr = str(duration)+unit
		timestamp=datetime.datetime.today().strftime("%Y%m%d.%H%M")
		name="auto-"+timestamp+"-"+durationStr
		return self.fs.create_zfs_snapshot(name=name)

	@staticmethod
	def Get_snapshot_time(snapshot_name):
		if snapshot_name.startswith("auto-"):
			components=snapshot_name.split("-")
			return datetime.datetime.strptime(components[1], "%Y%m%d.%H%M")

		return None

	@staticmethod
	def Get_time_delta(deltaStr):
		if len(deltaStr) < 2:
			raise ValueError("Invalid duration")

		duration=int(deltaStr[:len(deltaStr)-1])
		if duration < 1:
			raise ValueError("Duration must be strictly positive")

		unit=deltaStr[len(deltaStr)-1]
		if not unit in ["h", "d", "w", "y"]:
			raise ValueError("Invalid duration")

		delta={
			'h': lambda x: datetime.timedelta(hours=x),
			'd': lambda x: datetime.timedelta(days=x),
			'w': lambda x: datetime.timedelta(weeks=x),
			'y': lambda x: datetime.timedelta(years=x),
		}[unit](duration)
		return delta

	@staticmethod
	def Get_snapshot_expiration(snapshot_name):
		if snapshot_name.startswith("auto-"):
			components=snapshot_name.split("-")
			timestamp=datetime.datetime.strptime(components[1], "%Y%m%d.%H%M")
			delta = TimeSnapshots.Get_time_delta(components[2])
			if delta != None:
				return timestamp+delta

		return None

	def get_expired_snapshots(self):
		snapshot_list=[]
		for snapshot in self.fs.get_snapshots():
			skip=False
			snapshot_name=snapshot.split("@")[1]
			try:
				expire_timestamp = TimeSnapshots.Get_snapshot_expiration(snapshot_name)
				if expire_timestamp != None and datetime.datetime.today() > expire_timestamp:
					snapshot_list.append(snapshot)
			except ValueError:
				pass

		return snapshot_list

	def expire_snapshots(self):
		snapshot_list=self.get_expired_snapshots()
		for snap_to_remove in snapshot_list:
			if not self.fs.destroy_snapshot(snap_to_remove=snap_to_remove):
				return False

		return True




def get_process_list(remote=[]):
	ps = subprocess.check_output(remote + ["ps", "aux"], universal_newlines=True)
	processes = ps.split('\n')
	nfields = len(processes[0].split()) - 1
	def proc_split(row):
		return row.split(None,nfields)
	return map(proc_split,processes[1:-1])

class TimeOut(Exception):
		def __init__(self):
				Exception.__init__(self,'Timeout ')

def _raise_TimeOut(sig, stack):
		raise TimeOut()

class TimeoutObject(object):
		def __init__(self, timeout, raise_exception=True):
				self.timeout = timeout
				self.raise_exception = raise_exception

		def __enter__(self):
				self.old_handler = signal.signal(signal.SIGALRM, _raise_TimeOut)
				signal.alarm(self.timeout)

		def __exit__(self, exc_type, exc_val, exc_tb):
				signal.signal(signal.SIGALRM, self.old_handler)
				signal.alarm(0)
				if exc_type is not TimeOut:
						return False
				return not self.raise_exception

def get_pids_for_cmd_line_parts(remote=[],cmd_line_parts=[]):
	pids=[]
	for line in get_process_list(remote=remote):
		if len(line)<=1:
			continue
		for part in cmd_line_parts:
			if part not in line[-1]:
				break
		else:
			pids.append(line[1])
	return pids

def waitfor_cmd_to_exit(remote=[], cmd_line_parts=[], sleep=5):
	pids=get_pids_for_cmd_line_parts(remote=remote,cmd_line_parts=cmd_line_parts)
	if len(pids)>0:
		while True:
			running=get_process_list(remote=remote)
			for line in running:
				if line[1] in pids:
					time.sleep(sleep)
					break # exit for loop
			else:
				break #no process found, exit while loop

