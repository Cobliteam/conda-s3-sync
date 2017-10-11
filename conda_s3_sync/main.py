from __future__ import unicode_literals

import argparse
import datetime
import json
import os.path
import re
import shutil
import subprocess
import tempfile
import time
try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = os.devnull

import boto3
import iso8601


def zip_dicts_by_key(*dicts):
    keys = set()
    for d in dicts:
        keys.update(d.keys())

    for key in keys:
        values = tuple(d.get(key) for d in dicts)
        yield key, values


class CondaS3Sync(object):
    def __init__(self, conda_bin, s3_client, s3_bucket, s3_path,
                 path_filter=None, include_root=False):
        self.conda_bin = conda_bin
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.path_filter = path_filter and re.compile(path_filter)
        self.include_root = include_root

        self._conda_info = None

    def get_conda_info(self):
        if self._conda_info is None:
            out = subprocess.check_output(
                [self.conda_bin, 'info', '-e', '--json'],
                stdin=DEVNULL)

            self._conda_info = json.loads(out)

        return self._conda_info

    def _is_env_accepted(self, path):
        return (not self.path_filter or
                not self.path_filter.search(os.path.normpath(path)))

    def get_conda_envs(self):
        info = self.get_conda_info()
        for path in info['envs']:
            if not self._is_env_accepted(path):
                continue

            yield os.path.basename(path), path

        if self.include_root:
            yield 'root', info['root_prefix']

    def export_conda_env(self, env_path, export_path):
        with open(export_path, 'wb') as f:
            out = subprocess.check_output(
                [self.conda_bin, 'env', 'export', '-p', env_path],
                stdin=DEVNULL)
            f.write(out)

    def _get_env_name_for_path(self, path):
        fname = os.path.basename(path)
        env_name, ext = os.path.splitext(fname)
        if ext not in ('.yml', '.yaml'):
            return None

        return env_name

    def _get_env_path_for_name(self, env_name):
        return next((path for path in self.get_conda_info()['envs']
                     if os.path.basename(path) == env_name), None)

    def update_conda_env(self, export_path, base_path=None, prune=False,
                         last_modified=None):
        env_name = self._get_env_name_for_path(export_path)
        if not env_name:
            raise ValueError('Invalid environment file, must have YAML '
                             'extension')

        existing_path = self._get_env_path_for_name(env_name)
        if not existing_path and not base_path:
            # Figure out the path later
            env_path = None
            env_opts = ['-n', env_name]
        else:
            if existing_path:
                env_path = existing_path
            else:
                env_path = os.path.join(base_path, env_name)
            env_opts = ['-p', env_path]

        if existing_path:
            cmd = [self.conda_bin, 'env', 'update', '-f', export_path]
            if prune:
                cmd.append('--prune')
        else:
            cmd = [self.conda_bin, 'env', 'create', '-f', export_path]

        cmd.extend(env_opts)
        subprocess.check_call(cmd, stdin=DEVNULL)

        # Reset cache information, since we made changes to the envs
        self._conda_info = None
        if env_path is None:
            env_path = self._get_env_path_for_name(env_name)

        if last_modified:
            history_path = os.path.join(env_path, 'conda-meta', 'history')
            mtime = time.mktime(last_modified.timetuple())
            os.utime(history_path, (mtime, mtime))

    def _get_env_last_modified(self, path):
        history_path = os.path.join(path, 'conda-meta', 'history')
        mtime = os.path.getmtime(history_path)

        return datetime.datetime.fromtimestamp(mtime, datetime.timezone.utc)

    def export_conda_envs(self):
        tmp_dir = tempfile.mkdtemp()
        envs = {}

        for name, path in self.get_conda_envs():
            export_path = os.path.join(tmp_dir, name + '.yml')
            mtime = self._get_env_last_modified(path)
            self.export_conda_env(path, export_path)

            envs[name] = (export_path, mtime)

        return tmp_dir, envs

    def download_remote_envs(self):
        tmp_dir = tempfile.mkdtemp()
        envs = {}

        bucket = self.s3_client.Bucket(self.s3_bucket)
        for obj in bucket.objects.all():
            path, fname = os.path.split(obj.key)
            if path != self.s3_path:
                continue

            env_name = self._get_env_name_for_path(obj.key)
            if not env_name:
                continue

            export_path = os.path.join(tmp_dir, fname)
            actual_obj = self.s3_client.Object(obj.bucket_name, obj.key)
            actual_obj.download_file(export_path)

            last_modified = actual_obj.metadata.get('conda-env-last-modified')
            if last_modified:
                last_modified = iso8601.parse_date(last_modified)
            else:
                last_modified = actual_obj.last_modified

            envs[env_name] = (export_path, last_modified)

        return tmp_dir, envs

    def sync_all(self):
        local_tmp_dir = remote_tmp_dir = None
        try:
            local_tmp_dir, local_envs = self.export_conda_envs()
            remote_tmp_dir, remote_envs = self.download_remote_envs()
            bucket = self.s3_client.Bucket(self.s3_bucket)

            for env_name, (local, remote) in zip_dicts_by_key(local_envs,
                                                              remote_envs):
                local_path, local_mtime = local or (None, None)
                remote_path, remote_mtime = remote or (None, None)

                push = pull = False
                if local and remote:
                    if local_mtime > remote_mtime:
                        push = True
                    elif local_mtime < remote_mtime:
                        pull = True
                elif local:
                    push = True
                elif remote:
                    pull = True

                if push:
                    key = os.path.join(
                        self.s3_path, os.path.basename(local_path))
                    mtime_s = local_mtime.isoformat('T')
                    metadata = {'conda-env-last-modified': mtime_s}
                    bucket.upload_file(local_path, key,
                                       ExtraArgs={'Metadata': metadata})
                elif pull:
                    self.update_conda_env(remote_path,
                                          last_modified=remote_mtime)

        finally:
            if local_tmp_dir:
                shutil.rmtree(local_tmp_dir)
            if remote_tmp_dir:
                shutil.rmtree(remote_tmp_dir)


def parse_s3_location(loc):
    loc = re.sub(r'^s3://', '', loc)
    bucket, path = loc.split('/', 1)
    path = re.sub(r'/+$', '', path)

    return bucket, path


def main():
    argp = argparse.ArgumentParser(
        description='Synchronize Anaconda environments information to S3')
    argp.add_argument(
        '--path-filter',
        default=None, metavar='REGEX',
        help='Regular expression of env paths to include')
    argp.add_argument(
        '--conda-bin',
        default='conda', metavar='PATH',
        help='Path to conda-binary')
    argp.add_argument(
        '--include-root-env',
        action='store_true',
        help='Include root Anaconda environment in addition to any custom '
             'envs')
    argp.add_argument(
        's3_location',
        metavar='BUCKET[/PATH]',
        help='Bucket and path of S3 location to synchronize to/from')

    args = argp.parse_args()

    bucket, path = parse_s3_location(args.s3_location)
    s3_client = boto3.resource('s3')

    sync = CondaS3Sync(
        conda_bin=args.conda_bin,
        s3_client=s3_client,
        s3_bucket=bucket,
        s3_path=path,
        path_filter=args.path_filter,
        include_root=args.include_root_env)
    sync.sync_all()


if __name__ == '__main__':
    main()
