import json
import multiprocessing
import os
import re
import shutil
import subprocess as sp
import traceback
from dataclasses import asdict, dataclass, field
from random import shuffle
from tempfile import TemporaryDirectory
from time import time
from typing import Union, List, ContextManager

import utils
from bucket_dict import LazyBucketDict


@dataclass
class PackageJob:
    bucket: str
    name: str
    version: str
    url: Union[None, str]
    sha256: Union[None, str]
    idx: int
    timeout: int = field(default=60)
    py_versions: list = field(default_factory=list)
    drv: str = None


@dataclass
class JobResult:
    name: str
    version: str
    py_ver: str
    error: Union[None, str] = None
    install_requires: Union[None, str, list, dict] = field(default_factory=list)
    setup_requires: Union[None, str, list, dict] = field(default_factory=list)
    extras_require: Union[None, str, list, dict] = field(default_factory=list)
    tests_require: Union[None, str, list, dict] = field(default_factory=list)
    python_requires: Union[None, str, list, dict] = field(default_factory=list)


@dataclass
class PKG:
    install_requires: str
    setup_requires: str
    extras_require: str
    tests_require: str
    python_requires: str


def compute_drvs(jobs: List[PackageJob], extractor_src, store=None):
    extractor_jobs = list(dict(
        pkg=job.name,
        version=job.version,
        url=job.url,
        sha256=job.sha256,
        pyVersions=job.py_versions,
    ) for job in jobs)
    with TemporaryDirectory() as tempdir:
        jobs_file = f"{tempdir}/jobs.json"
        with open(jobs_file, 'w') as f:
            json.dump(extractor_jobs, f)
        os.environ['EXTRACTOR_JOBS_JSON_FILE'] = jobs_file
        cmd = ["nix", "eval", "--impure", "-f", f"{extractor_src}/make-drvs.nix",]
        if store:
            cmd += ["--store", store]
        print(' '.join(cmd).replace(' "', ' \'"').replace('" ', '"\' '))
        try:
            nix_eval_result = sp.run(cmd, capture_output=True, check=True)
        except sp.CalledProcessError as e:
            print(e.stderr)
            raise
        result = json.loads(json.loads(nix_eval_result.stdout))
    for job in jobs:
        job.drv = result[f"{job.name}#{job.version}"]


def format_error(log: str, pkg_version):
    """
    Execute some replacement transformations on the log,
    to make it more compressible
    """

    # remove store hashes
    log = re.subn(r"(.*/nix/store/)[\d\w]+(-.*)", r"\1#hash#\2", log)[0]

    # reduce multiple white spaces to a single space
    log = re.subn(r"(\S*)( +)(\S*)", r"\1 \3", log)[0]

    # remove python versions
    log = re.sub("python[\d\.\-ab]+", "python#VER#", log)

    # remove line numbers
    log = re.sub("line (\d*)", "line #NUM#", log)

    # equalize tmp directories
    log = re.sub("tmp[\d\w_]*", "#TMP#", log)

    # remove package versions
    log = re.sub(pkg_version, "#PKG_VER#", log)

    # detect some common errors and shorten them
    common = (
        'unpacker produced multiple directories',
    )
    for err in common:
        if err in log:
            log = err
            break

    # for Exceptions keep only short text
    match = re.match("(?s:.*)[\s\n]([\w\._]*Error:.*)", log)
    if match:
        log = match.groups()[0]

    # remove common warnings and trim line number and length
    lines = log.splitlines(keepends=True)
    lines = map(lambda line: line[:400], lines)
    remove_lines_marker = (
        '/homeless-shelter/.cache/pip/http',
        '/homeless-shelter/.cache/pip',
        'DEPRECATION: Python 2.7'
    )
    filtered = filter(lambda l: not any(marker in l for marker in remove_lines_marker), lines)
    return ''.join(list(filtered)[:90])


def extract_requirements(job: PackageJob, deadline, total_num, store=None):
    try:
        if deadline and time() > deadline:
            raise Exception("Deadline occurred. Skipping this job")
        print(f"Bucket {job.bucket} - Job {job.idx+1}/{total_num} - "
              f"{job.name}:{job.version}     (py: {' '.join(job.py_versions)})")
        with TemporaryDirectory() as tempdir:
            out_dir = f"{tempdir}/json"
            cmd = ["nix-build", job.drv, "-o", out_dir]
            if store:
                cmd += ["--store", store]
            # print(' '.join(cmd).replace(' "', ' \'"').replace('" ', '"\' '))
            try:
                sp.run(cmd, capture_output=True, timeout=job.timeout, check=True)
            except (sp.CalledProcessError, sp.TimeoutExpired) as e:
                print(f"problem with {job.name}:{job.version}\n{e.stderr.decode()}")
                formatted = format_error(e.stderr.decode(), job.version)
                # in case GC didn't kick in early enough, we need to ignore the results
                if any(s in formatted for s in (
                        "o space left on device",
                        "lack of free disk space")):
                    return e
                return [JobResult(
                    name=job.name,
                    version=job.version,
                    py_ver=f"{py_ver}",
                    error=formatted,
                ) for py_ver in job.py_versions]
            results = []
            for py_ver in job.py_versions:
                data = None
                try:
                    path = os.readlink(f"{out_dir}")
                    if store:
                        path = path.replace('/nix/store', f"{store}/nix/store")
                    with open(f"{path}/python{py_ver}.json") as f:
                        content = f.read().strip()
                        if content != '':
                            data = json.loads(content)
                except FileNotFoundError:
                    pass
                if data is None:
                    with open(f"{path}/python{py_ver}.log") as f:
                        error = format_error(f.read(), job.version)
                    print(error)
                    results.append(JobResult(
                        name=job.name,
                        version=job.version,
                        py_ver=f"{py_ver}",
                        error=error,
                    ))
                else:
                    for k in ('name', 'version'):
                        if k in data:
                            del data[k]
                    results.append(JobResult(
                        name=job.name,
                        version=job.version,
                        py_ver=py_ver,
                        **data
                    ))
            return results
    except Exception as e:
        traceback.print_exc()
        return e


def get_jobs(pypi_index, error_dict, pkgs_dict, bucket, py_vers, limit_num=None, limit_names=None):
    jobs: List[PackageJob] = []
    names = list(pypi_index.by_bucket(bucket).keys())
    total_nr = 0
    for pkg_name in names:
        if limit_names and pkg_name not in limit_names:
            continue
        for ver, release_types in pypi_index[pkg_name].items():
            if 'sdist' not in release_types:
                continue
            total_nr += 1
            # collect python versions for which no data exists yet
            required_py_vers = []
            for pyver in py_vers:
                try:
                    pkgs_dict[pkg_name][ver][pyver]
                except KeyError:
                    try:
                        error_dict[pkg_name][ver][pyver]
                    except KeyError:
                        # there is no data or error for that pkg release + pyver -> need to crawl
                        required_py_vers.append(pyver)
            if not required_py_vers:
                continue
            release = release_types['sdist']
            jobs.append(PackageJob(
                bucket,
                pkg_name,
                ver,
                f"https://files.pythonhosted.org/packages/source/{pkg_name[0]}/{pkg_name}/{release[1]}",
                release[0],
                0,
                py_versions=required_py_vers,
            ))
    # because some packages are significantly bigger than others, we shuffle all jobs
    # to prevent fluctuations in CPU usage
    shuffle(jobs)

    # When support for a new python version was added, the amount of jobs is massive.
    # We want to ensure that new packages are prioritized before old packages.
    # To identify new packages, we use the number of python versions that need to be updated for that package.
    jobs.sort(key=lambda j: -len(j.py_versions))

    # limit number of jobs
    if limit_num:
        jobs = jobs[:limit_num]

    # assign numbers
    for i, job in enumerate(jobs):
        job.idx = i

    print(f"Bucket {bucket}: {len(jobs)} out of {total_nr} total sdist releases need to be updated")
    return jobs


def get_processed():
    with open('/tmp/jobs', 'r') as f:
        return {tuple(t) for t in json.load(f)}


def build_base(extractor_src, py_vers, store=None):
    name = 'requests'
    version = '2.22.0'
    url = 'https://files.pythonhosted.org/packages/01/62/' \
          'ddcf76d1d19885e8579acb1b1df26a852b03472c0e46d2b959a714c90608/requests-2.22.0.tar.gz'
    sha256 = '11e007a8a2aa0323f5a921e9e6a2d7e4e67d9877e85773fba9ba6419025cbeb4'
    cmd = [
        "nix-build", f"{extractor_src}/fast-extractor.nix",
        "--arg", "url", f'"{url}"',
        "--arg", "sha256", f'"{sha256}"',
        "--arg", "pkg", f'"{name}"',
        "--arg", "version", f'"{version}"',
        "--arg", "pyVersions", f'''[ {" ".join(map(lambda p: f'"{p}"', py_vers))} ]''',
        "--no-out-link",
    ]
    if store:
        cmd += ["--store", f"{store}"]
    sp.check_call(cmd, timeout=1000)



def pkg_to_dict(pkg):
    pkg_dict = asdict(PKG(
        install_requires=pkg.install_requires,
        setup_requires=pkg.setup_requires,
        extras_require=pkg.extras_require,
        tests_require=pkg.tests_require,
        python_requires=pkg.python_requires
    ))
    new_release = {}
    for key, val in pkg_dict.items():
        if not val:
            continue
        if key == 'extras_require':
            for extra_key, extra_reqs in val.items():
                val[extra_key] = list(flatten_req_list(extra_reqs))
        if key not in flatten_keys:
            new_release[key] = val
            continue
        val = list(flatten_req_list(val))
        if isinstance(val, str):
            val = [val]
        if not all(isinstance(elem, str) for elem in val):
            print(val)
            raise Exception('Requirements must be list of strings')
        new_release[key] = val
    return new_release


def flatten_req_list(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, list):
        if len(obj) == 0:
            return
        elif len(obj) == 1:
            for s in flatten_req_list(obj[0]):
                yield s
        else:
            for elem in obj:
                for s in flatten_req_list(elem):
                    yield s
    else:
        raise Exception('Is not list or str')


flatten_keys = (
    'setup_requires',
    'install_requires',
    'tests_require',
    'python_requires',
)


def insert(py_ver, name, ver, release, target, error=""):
    if error:
        release = error
    ver = ver.strip()
    # create structure
    if name not in target:
        target[name] = {}
    if ver not in target[name]:
        target[name][ver] = {}
    target[name][ver][py_ver] = release


def sort_key_pyver(pyver):
    return len(pyver), pyver


def compress_dict(d):
    items = sorted(d.items(), key=lambda x: sort_key_pyver(x[0]))
    keep = {}
    for k, v in items:
        for keep_key, keep_val in keep.items():
            if v == keep_val:
                d[k] = keep_key
                break
        if not isinstance(d[k], str) or d[k] not in keep:
            keep[k] = v


def decompress_dict(d):
    keys = set(d.keys())
    for k, v in d.items():
        if isinstance(v, str) and v in keys:
            d[k] = d[v]


def compress(pkgs_dict: LazyBucketDict):
    for name, vers in pkgs_dict.items():
        for ver, pyvers in vers.items():
            compress_dict(pyvers)
        compress_dict(vers)


def decompress(pkgs_dict: LazyBucketDict):
    for name, vers in pkgs_dict.items():
        decompress_dict(vers)
        for ver, pyvers in vers.items():
            decompress_dict(pyvers)


def purge(pypi_index, pkgs_dict: LazyBucketDict, bucket, py_vers):
    # purge all versions which are not on pypi anymore
    for name, vers in pkgs_dict.by_bucket(bucket).copy().items():
        if name not in pypi_index:
            print(f"deleting package {name} from DB because it has been removed from pypi")
            del pkgs_dict[name]
            continue
        for ver in tuple(vers.keys()):
            if ver not in pypi_index[name]:
                print(f"deleting package {name} version {ver} from DB because it has been removed from pypi")
                del pkgs_dict[name][ver]
    # purge old python versions
    for name, vers in pkgs_dict.by_bucket(bucket).copy().items():
        for ver, pyvers in vers.copy().items():
            for pyver in tuple(pyvers.keys()):
                if pyver not in py_vers:
                    print(f"deleting package {name} version {ver} for python {pyver}"
                          f" from DB because we dropped support for this python version")
                    del pkgs_dict[name][ver][pyver]
            if len(pkgs_dict[name][ver]) == 0:
                print(f"deleting package {name} version {ver} from DB"
                      f" because it is not compatible with any of our supported python versions")
                del pkgs_dict[name][ver]
        if len(pkgs_dict[name]) == 0:
            print(f"deleting package {name} from DB"
                  f" because it has no releases left which are compatible with any of our supported python versions")
            del pkgs_dict[name]


class Measure(ContextManager):
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.enter_time = time()
        print(f'beginning "{self.name}"')
    def __exit__(self, exc_type, exc_val, exc_tb):
        dur = round(time() - self.enter_time, 1)
        print(f'"{self.name}" took {dur}s')


def main():
    # settings related to performance/parallelization
    amount_buckets = int(os.environ.get('AMOUNT_BUCKETS', "256"))
    limit_names = set(filter(lambda n: bool(n), os.environ.get('LIMIT_NAMES', "").split(',')))
    max_minutes = int(os.environ.get('MAX_MINUTES', "0"))
    bucket_jobs = int(os.environ.get('BUCKET_JOBS', "0"))
    start_bucket = int(os.environ.get('BUCKET_START', "0"))
    workers = int(os.environ.get('WORKERS', multiprocessing.cpu_count() * 2))

    # general settings
    dump_dir = os.environ.get('DUMP_DIR', "./sdist")
    extractor_src = os.environ.get("EXTRACTOR_SRC")
    if not extractor_src:
        raise Exception("Set env variable 'EXTRACTOR_SRC to {mach-nix}/lib/extractor'")
    min_free_gb = int(os.environ.get('MIN_FREE_GB', "0"))
    py_vers_short = os.environ.get('PYTHON_VERSIONS', "27,36,37,38,39,310,311").strip().split(',')
    pypi_fetcher_dir = os.environ.get('PYPI_FETCHER', '/tmp/pypi_fetcher')
    store = os.environ.get('STORE', None)

    deadline_total = time() + max_minutes * 60 if max_minutes else None

    # cache build time deps, otherwise first job will be slow
    with Measure("ensure build time deps"):
        build_base(extractor_src, py_vers_short, store=store)

    garbage_collected = False

    for idx, bucket in enumerate(LazyBucketDict.bucket_keys()):
        # calculate per bucket deadline if MAX_MINUTES is used
        if deadline_total:
            amount = min(amount_buckets, 256 - start_bucket)
            deadline = time() + (deadline_total - time()) / amount
        else:
            deadline = None
        if idx < start_bucket or idx >= start_bucket + amount_buckets:
            continue
        pkgs_dict = LazyBucketDict(dump_dir, restrict_to_bucket=bucket)
        pypi_index = LazyBucketDict(f"{pypi_fetcher_dir}/pypi", restrict_to_bucket=bucket)
        # load error data
        error_dict = LazyBucketDict(dump_dir + "-errors", restrict_to_bucket=bucket)
        decompress(error_dict.by_bucket(bucket))
        with Measure('Get processed pkgs'):
            print(f"DB contains {len(list(pkgs_dict.keys()))} pkgs at this time for bucket {bucket}")
        with Measure("decompressing data"):
            decompress(pkgs_dict.by_bucket(bucket))
        # purge data for old python versions and packages which got deleted from pypi
        with Measure("purging packages"):
            purge(pypi_index, pkgs_dict, bucket, py_vers_short)
        with Measure("getting jobs"):
            jobs = get_jobs(
                pypi_index, error_dict, pkgs_dict, bucket, py_vers_short, limit_num=bucket_jobs, limit_names=limit_names)
            if not jobs:
                continue
            compute_drvs(jobs, extractor_src, store=store)

        # ensure that all the build time dependencies are cached before starting,
        # otherwise jobs might time out
        if garbage_collected:
            with Measure("ensure build time deps"):
                build_base(extractor_src, py_vers_short, store=store)
        with Measure('executing jobs'):
            if workers > 1:
                pool_results = utils.parallel(
                    extract_requirements,
                    (
                        jobs,
                        (deadline,) * len(jobs),
                        (len(jobs),) * len(jobs),
                        (store,) * len(jobs)
                    ),
                    workers=workers,
                    use_processes=False)
            else:
                pool_results = [extract_requirements(args, deadline, store) for args in jobs]

        # filter out exceptions
        results = []
        for i, res in enumerate(pool_results):
            if not isinstance(res, Exception):
                for r in res:
                    results.append(r)

        # insert new data
        for pkg in sorted(results, key=lambda pkg: (pkg.name, pkg.version, sort_key_pyver(pkg.py_ver))):
            py_ver = ''.join(filter(lambda c: c.isdigit(), pkg.py_ver))
            if pkg.error:
                target = error_dict
            else:
                target = pkgs_dict
            insert(py_ver, pkg.name, pkg.version, pkg_to_dict(pkg), target, error=pkg.error)

        # compress and save
        with Measure("compressing data"):
            compress(pkgs_dict.by_bucket(bucket))
            compress(error_dict.by_bucket(bucket))
        print("finished compressing data")
        with Measure("saving data"):
            pkgs_dict.save()
            error_dict.save()

        # collect garbage if free space < MIN_FREE_GB
        if shutil.disk_usage(store or "/nix/store").free / (1000 ** 3) < min_free_gb:
            with Measure("collecting nix store garbage"):
                sp.run(
                    f"nix-collect-garbage {f'--store {store}' if store else ''}",
                    capture_output=True,
                    shell=True
                )
                garbage_collected = True

        # stop execution if deadline occurred
        if deadline_total and time() > deadline_total:
            print(f"Deadline occurred. Stopping execution. Last Bucket was {bucket}")
            break


if __name__ == "__main__":
    main()
