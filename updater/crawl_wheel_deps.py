import os
import sys
import traceback
import zipfile
from dataclasses import dataclass
from os.path import isdir
from random import shuffle
from tempfile import NamedTemporaryFile
from time import sleep, time
from typing import Union, Dict
import subprocess as sp

import pkginfo
import requests
from bucket_dict import LazyBucketDict
from utils import parallel

email = sp.check_output("git config --get user.email", shell=True).strip()
headers = {'User-Agent': f'Pypi Daily Sync (Contact: {email})'}


@dataclass
class Job:
    name: str
    ver: str
    filename: str
    pyver: str
    url: str
    nr: int
    bucket: str
    bucket_seq: int = 0


@dataclass()
class Result:
    job: Job
    requires_dist: str
    provides_extras: str
    requires_external: str
    requires_python: str


class Retry(Exception):
    pass


def construct_url(name, pyver, filename: str):
    base_url = "https://files.pythonhosted.org/packages/"
    return f"{base_url}{pyver}/{name[0]}/{name}/{filename}"


def mine_wheel_metadata_full_download(job: Job, tmp_dir) -> Union[Result, Exception]:
    print(f"Bucket {job.bucket} (seq {job.bucket_seq}) - Job {job.nr+1} - {job.name}:{job.ver}")
    for _ in range(5):
        try:
            with NamedTemporaryFile(suffix='.whl', dir=tmp_dir) as f:
                resp = requests.get(job.url, headers=headers)
                if resp.status_code == 404:
                    return requests.HTTPError()
                if resp.status_code in [503, 502]:
                    try:
                        resp.raise_for_status()
                    except:
                        traceback.print_exc()
                    raise Retry
                resp.raise_for_status()
                with open(f.name, 'wb') as f_write:
                    f_write.write(resp.content)
                metadata = pkginfo.get_metadata(f.name)
            return Result(
                job=job,
                requires_dist=metadata.requires_dist,
                provides_extras=metadata.provides_extras,
                requires_external=metadata.requires_external,
                requires_python=metadata.requires_python,
            )
        except Retry:
            sleep(10)
        except zipfile.BadZipFile as e:
            return e
        except Exception:
            print(f"Problem with {job.name}:{job.ver}")
            traceback.print_exc()
            raise


def is_done(dump_dict, pkg_name, pkg_ver, pyver, filename):
    try:
        dump_dict[pkg_name][pyver][pkg_ver][filename]
    except KeyError:
        return False
    else:
        return True


def get_jobs(bucket, pypi_dict:LazyBucketDict, dump_dict: LazyBucketDict):
    names = list(pypi_dict.by_bucket(bucket).keys())
    jobs = []
    for pkg_name in names:
        for ver, release_types in pypi_dict[pkg_name].items():
            if 'wheels' not in release_types:
                continue
            for filename, data in release_types['wheels'].items():
                pyver = data[1]
                if is_done(dump_dict, pkg_name, ver, pyver, filename):
                    continue
                url = construct_url(pkg_name, pyver, filename)
                jobs.append(dict(
                    name=pkg_name, ver=ver, filename=filename, pyver=pyver,
                    url=url, bucket=bucket))
    shuffle(jobs)
    return [Job(**j, nr=idx) for idx, j in enumerate(jobs)]


def sort(d: dict):
    res = {}
    for k, v in sorted(d.items()):
        if isinstance(v, dict):
            res[k] = sort(v)
        else:
            res[k] = v
    return res


def decompress(d):
    for name, pyvers in d.items():
        for pyver, pkg_vers in pyvers.items():
            for pkg_ver, fnames in pkg_vers.items():
                for fn, data in fnames.items():
                    if isinstance(data, str):
                        key_ver, key_fn = data.split('@')
                        try:
                            pkg_vers[key_ver][key_fn]
                        except KeyError:
                            print(f"Error with key_ver: {key_ver} , key_fn: {key_fn}")
                            exit()
                        fnames[fn] = pkg_vers[key_ver][key_fn]


def compress(dump_dict):
    decompress(dump_dict)
    # sort
    for k, v in dump_dict.items():
        dump_dict[k] = sort(v)
    for name, pyvers in dump_dict.items():
        for pyver, pkg_vers in pyvers.items():

            all_fnames = {}
            for pkg_ver, fnames in pkg_vers.items():
                for fn, data in fnames.items():
                    for existing_key, d in all_fnames.items():
                        if data == d:
                            fnames[fn] = existing_key
                            break
                    if not isinstance(fnames[fn], str):
                        all_fnames[f"{pkg_ver}@{fn}"] = data


def exec_or_return_exc(func, job, *args):
    try:
        return func(job, *args)
    except Exception as e:
        traceback.print_exc()
        return e


def prune_entries(bucket, pypi_dict, dump_dict):
    """
    Since the wheel data set is updated incrementally, we need to check
    if existing entries have been deleted from pypi and prune them accordingly.
    """
    def fn_in_pypi(name, pkg_ver, fn):
        if name in pypi_dict\
                and pkg_ver in pypi_dict[name]\
                and 'wheels' in pypi_dict[name][pkg_ver]\
                and fn in pypi_dict[name][pkg_ver]['wheels']:
            return True
        return False

    to_delete = []
    # get to delete
    for name in dump_dict.keys(bucket):
        for py_ver, pkg_vers in dump_dict[name].items():
            delete = False
            for pkg_ver, fnames in pkg_vers.items():
                for fn in fnames.keys():
                    if not fn_in_pypi(name, pkg_ver, fn):
                        delete = True
                        break
                if delete:
                    to_delete.append((name, py_ver))
                    break
    # delete
    for name, py_ver in to_delete:
        print(f"deleting {name}:{py_ver}")
        del dump_dict[name][py_ver]
    for name, _ in to_delete:
        if name in dump_dict and not len(dump_dict[name]):
            del dump_dict[name]


def process_bucket_result(bucket, result, dump_dir):
    dump_dict = LazyBucketDict(dump_dir, restrict_to_bucket=bucket)
    for r in result:
        if isinstance(r, Exception):
            continue
        name = r.job.name
        ver = r.job.ver
        pyver = r.job.pyver
        fn = r.job.filename
        if name not in dump_dict:
            dump_dict[name] = {}
        if pyver not in dump_dict[name]:
            dump_dict[name][pyver] = {}
        if ver not in dump_dict[name][pyver]:
            dump_dict[name][pyver][ver] = {}
        dump_dict[name][pyver][ver][fn] = {}
        for key in ('requires_dist', 'provides_extras', 'requires_external', 'requires_python'):
            val = getattr(r, key)
            if val:
                dump_dict[name][pyver][ver][fn][key] = val
    compress(dump_dict)
    dump_dict.save()



def iter_jobs(jobs_by_bucket: Dict[str, list], deadline):
    for seq, (bucket, jobs) in enumerate(jobs_by_bucket.items()):
        print(f"Starting bucket {bucket} seq {seq} with {len(jobs)} jobs")
        for job in jobs:
            yield job
        if deadline and time() > deadline:
            print("Deadline occured. Stopping job execution")
            break
        


def main():
    dump_dir = os.environ.get('DUMP_DIR', "./wheel")
    max_minutes = int(os.environ.get('MAX_MINUTES', "0"))
    workers = int(os.environ.get('WORKERS', "1"))
    pypi_fetcher_dir = os.environ.get('PYPI_FETCHER')
    tmp_dir = os.environ.get('TMP_DIR', default=None)

    print(f'Index directory: {pypi_fetcher_dir}')
    assert isdir(pypi_fetcher_dir)

    deadline = time() + max_minutes * 60 if max_minutes else None
    
    jobs_by_bucket = {}
    buckets = list(LazyBucketDict.bucket_keys())
    shuffle(buckets)
    for seq, bucket in enumerate(buckets):
        pypi_dict = LazyBucketDict(f"{pypi_fetcher_dir}/pypi")
        dump_dict = LazyBucketDict(dump_dir, restrict_to_bucket=bucket)
        print(f"Prune bucket {bucket} seq {seq}")
        prune_entries(bucket, pypi_dict, dump_dict)
        dump_dict.save()
        print(f"Calculating jobs for bucket {bucket} seq {seq}")
        jobs_by_bucket[bucket] = get_jobs(bucket, pypi_dict, dump_dict)
        # assign sequence numbers for debug log
        for job in jobs_by_bucket[bucket]:
            job.bucket_seq = seq
    func = mine_wheel_metadata_full_download
    if workers > 1:
        def f(job):
            return exec_or_return_exc(func, job, tmp_dir)
        results = parallel(
            f,
            (iter_jobs(jobs_by_bucket, deadline),),
            workers=workers)
    else:
        results = [exec_or_return_exc(func, job, tmp_dir) for job in iter_jobs(jobs_by_bucket, deadline)]
    
    # split results back into bucket groups
    idx = 0
    for bucket, jobs in jobs_by_bucket.items():
        results_curr_bucket = []
        for job in jobs:
            results_curr_bucket.append(results[idx])
            idx += 1
        print(f"Saving results of bucket {bucket}")
        process_bucket_result(bucket, results_curr_bucket, dump_dir)
        if idx >= len(results):
            break
        
        


if __name__ == "__main__":
    main()
