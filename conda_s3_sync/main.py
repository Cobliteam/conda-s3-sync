from __future__ import print_function, unicode_literals

import argparse
import datetime
import json
import logging
import os.path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import yaml
try:
    from subprocess import DEVNULL
except ImportError:
    DEVNULL = os.devnull

import boto3
import iso8601


logger = logging.getLogger('conda-s3-sync')


def zip_dicts_by_key(*dicts):
    keys = set()
    for d in dicts:
        keys.update(d.keys())

    for key in keys:
        values = tuple(d.get(key) for d in dicts)
        yield key, values


class CondaError(RuntimeError):
    def __init__(self, message, data):
        super(CondaError, self).__init__(message)
        self.data = data


class CondaDependenciesError(CondaError):
    def __init__(self, message, data):
        super(CondaDependenciesError, self).__init__(message, data)
        self.bad_deps = data['bad_deps']


def replace_conda_dependency(data, check, replace):
    if isinstance(data, dict):
        return dict((k, replace_conda_dependency(v, check, replace))
                    for (k, v) in data.items())
    elif isinstance(data, list):
        return [replace_conda_dependency(v, check, replace)
                for v in data]
    elif isinstance(data, (str, bytes)) and check(data):
        return replace

    return data


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
            logger.info('Reloading Conda information')
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

            env_name = os.path.basename(path)
            logger.debug('Found Conda env: %s at %s', env_name, path)
            yield env_name, path

        if self.include_root:
            logger.debug('Found Root env at %s', path)
            yield 'root', info['root_prefix']

    def export_conda_env(self, env_path, export_path):
        with open(export_path, 'wb') as f:
            logger.debug('Exporting env %s to %s', env_path, export_path)
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

    def _run_conda_provision(self, env_file, *args, env_path=None,
                             env_name=None, prune=False, update=False):
        cmd = [self.conda_bin, 'env', 'update' if update else 'create',
               '--json', '-f', env_file]
        if env_path:
            cmd.extend(['-p', env_path])
        if env_name and (not update or not env_path):
            cmd.extend(['-n', env_name])

        logger.debug('Running conda: %s', cmd)
        proc = subprocess.Popen(cmd, stdin=DEVNULL, stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        proc.wait()
        try:
            err_data = json.loads(out)
        except ValueError:
            sys.stdout.buffer.write(out)
            err_data = None

        if proc.returncode != 0:
            if err_data and 'bad_deps' in err_data:
                raise CondaDependenciesError('Missing dependencies', err_data)

            raise CondaError('Update failed', err_data)

    def _run_conda_provision_retry(self, env_file, *args, **kwargs):
        failed_deps = set()
        while True:
            try:
                self._run_conda_provision(env_file, *args, **kwargs)
                break
            except CondaDependenciesError as e:
                logger.warn('Failed to install Conda environment. '
                            'Retrying installation by cleaning up broken '
                            'dependencies: %s', e.bad_deps)

                with open(env_file, 'r+') as f:
                    yaml_data = yaml.safe_load(f)
                    for dep in e.bad_deps:
                        dep_name = dep.split('=')[0]
                        if dep_name in failed_deps:
                            raise e

                        failed_deps.add(dep_name)
                        yaml_data = replace_conda_dependency(
                            yaml_data, lambda s: s.startswith(dep_name + '='),
                            dep_name)

                    f.seek(0)
                    yaml.dump(yaml_data, f)
                    f.truncate()
            except CondaError as e:
                logger.exception('Conda failed: %s', e.data)
                raise e

    def update_conda_env(self, export_path, base_path=None, prune=False,
                         last_modified=None):
        env_name = self._get_env_name_for_path(export_path)
        if not env_name:
            raise ValueError('Invalid environment file, must have YAML '
                             'extension')

        update = False
        existing_path = self._get_env_path_for_name(env_name)
        if not existing_path and not base_path:
            # Figure out the path later
            env_path = None
        else:
            if existing_path:
                env_path = existing_path
                update = True
            else:
                env_path = os.path.join(base_path, env_name)
                update = os.path.exists(env_path)

        self._run_conda_provision_retry(
            export_path, env_path=env_path, env_name=env_name, prune=prune,
            update=existing_path is not None)

        # Reset cache information, since we made changes to the envs
        self._conda_info = None
        if env_path is None:
            env_path = self._get_env_path_for_name(env_name)

        if last_modified:
            logger.debug('Updating env last-modified to %s', last_modified)

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

            logger.debug('Found remote environment at path %s', obj.key)

            export_path = os.path.join(tmp_dir, fname)
            actual_obj = self.s3_client.Object(obj.bucket_name, obj.key)
            actual_obj.download_file(export_path)

            last_modified = actual_obj.metadata.get('conda-env-last-modified')
            if last_modified:
                logger.debug('Got last-modified from metadata: %s',
                             last_modified)
                last_modified = iso8601.parse_date(last_modified)
            else:
                logger.debug('Missing last-modified metadata, using S3 '
                             'default: %s', actual_obj.last_modified)
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

                logger.debug('Synchronizing env %s: local_path=%s, '
                             'local_mtime=%s, remote_path=%s, remote_mtime=%s',
                             env_name, local_path, local_mtime, remote_path,
                             remote_mtime)

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
                    logger.debug('Pushing local venv to remote')

                    key = os.path.join(
                        self.s3_path, os.path.basename(local_path))
                    mtime_s = local_mtime.isoformat('T')
                    metadata = {'conda-env-last-modified': mtime_s}
                    bucket.upload_file(local_path, key,
                                       ExtraArgs={'Metadata': metadata})
                elif pull:
                    logger.debug('Pulling remote venv to local')

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
    logging.basicConfig(level=logging.WARN)
    logger.setLevel(logging.DEBUG)

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
