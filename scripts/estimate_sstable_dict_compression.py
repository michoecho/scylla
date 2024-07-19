#!/usr/bin/env python3

# Dependencies:
#
# - Will call `sudo` to mount and unmount the tmpfs.
# - Kernel 6.4, for tmpfs with `noswap`.
# - boto3, with appropriate credentials configured via the usual means
# - `zstd` in PATH
# - `scylla` in PATH
# - `free`, `grep`, `awk` in PATH

# Input:
# - An s3://... URL of a directory tree containing sstables.

# Output:
# - stdout: compression measurement results
# - stderr: logs

# Usage example:
#
# python this_script.py s3://my_bucket/path_to_backup >result.txt 2> >(tee log.txt >&2)
#
# (The script looks for sstables in the entire subtree.
# It will pick up both s3://my_bucket/path_to_backup/something-Data.db
# and s3://my_bucket/path_to_backup/a/b/something-Data.db).

# Explanation
#
# The script performs the following steps:
# - Creates the tmpfs mountpoint and mounts tmpfs to it.
# - Lists all *-Data.db files under the requested S3 URL.
# - From the files which meet size criteria, randomly chooses samples (with probability weighted by size) until DATA_TO_SAMPLE bytes are sampled.
# - Downloads each chosen sample (all sstable components of it), runs zstd's compression benchmarks on it, prints result to stdout, removes the sample.
# - Unmounts the tmpfs.

import subprocess
# Must fit in RAM. Limits the size of sstables that can be processed.
# TMPFS_SIZE = 8*1024*1024*1024
TMPFS_SIZE = int(0.85 * int(subprocess.check_output("free --bytes | awk '/^Mem/ {print $2}'", shell=True).strip()))
# This mountpoint wil be created in working directory. The name doesn't really matter, as long as it doesn't conflict with something that already exists.
TMPFS_MOUNT_NAME = "my_tmpfs"
# How many bytes' worth of sstables to choose from the directory for testing.
DATA_TO_SAMPLE = 100*1024*1024*1024
# It doesn't make sense to train dictionaries on overly small files. (Overfit).
# We will ignore sstables smaller than this.
MIN_SSTABLE_DATA_SIZE = 64*1024*1024
# We don't want to exceed the tmpfs (even after we create a decompressed copy), and we don't want to exceed the configured sample size too much.
# We will ignore sstables bigger than this.
MAX_SSTABLE_DATA_SIZE = min(int(TMPFS_SIZE*0.8), DATA_TO_SAMPLE)
# Path to the `scylla` binary to pass to the shell. Used for decompressing the sstables before the compression test.
SCYLLA = "scylla"

# Copy the above settings for logging purposes.
settings = dict(globals())

import boto3 
import contextlib
import itertools
import logging
import os
import pathlib
import random
import re
import shlex
import shutil
import subprocess
import sys
from collections import defaultdict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# For the lifetime of the context manager,
# sets up a tmpfs in working directory and chdirs to it.
@contextlib.contextmanager
def with_tmpfs(size):
    cwd = os.getcwd()
    if os.path.exists(TMPFS_MOUNT_NAME):
        os.rmdir(TMPFS_MOUNT_NAME)
    os.mkdir(TMPFS_MOUNT_NAME)
    subprocess.check_call(["sudo", "mount", "-o", fr"size={size}", "-o", "noswap", "-t", "tmpfs", "tmpfs", TMPFS_MOUNT_NAME])
    os.chdir(TMPFS_MOUNT_NAME)
    try:
        yield
    finally:
        os.chdir(cwd)
        subprocess.check_call(["sudo", "umount", TMPFS_MOUNT_NAME])
        os.rmdir(TMPFS_MOUNT_NAME)

# Equivalent of `aws s3 ls --recursive`
def list_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page['Contents']:
            yield obj

def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    random.seed(0)

    # Validate dependencies.
    subprocess.check_call([fr"{shlex.quote(SCYLLA)} --version"], shell=True, stdout=sys.stderr)
    subprocess.check_call([fr"zstd --version"], shell=True, stdout=sys.stderr)

    # Parse the s3 URL given in argv[1].
    parse_result = urlparse(sys.argv[1])
    assert parse_result.scheme == "s3"
    BUCKET = parse_result.netloc
    PREFIX = parse_result.path.lstrip("/")
    logger.info(fr"BUCKET: {BUCKET}")
    logger.info(fr"PREFIX: {PREFIX}")

    # Log the tunable constants.
    for k, v in settings.items():
        if not k.startswith("__"):
            logger.info(fr"{k}: {v}")

    client = boto3.client("s3")

    with with_tmpfs(TMPFS_SIZE):
        # Find out the compressed sizes and (estimated) uncompressed sizes of Data.db files.
        compressed_sizes: dict[str, int] = {}
        uncompressed_sizes: defaultdict[str, int] = defaultdict(int)
        total_size = 0
        data_db_regex = re.compile("^(.*)-Data.db$")
        compressioninfo_db_regex = re.compile("^(.*)-CompressionInfo.db$")
        for obj in list_objects(client, BUCKET, PREFIX):
            size = obj["Size"]
            if m := data_db_regex.fullmatch(obj["Key"]):
                sst = m.group(1)
                compressed_sizes[sst] = size
            if m := compressioninfo_db_regex.fullmatch(obj["Key"]):
                sst = m.group(1)
                # Assumes that compression chunks are 4 kiB.
                # Worst case the decompression will fail with ENOSPC.
                uncompressed_sizes[sst] = size // 8 * 4096
            total_size += size
        logger.info(fr"Total size of files under the URL: {total_size}")

        for k, v in compressed_sizes.items():
            logger.info(fr"Found s3://{BUCKET}/{k}-Data.db: {v} B compressed, {uncompressed_sizes[k]} B uncompressed")

        # Filter out files which don't meet the size criteria.
        viable: list[tuple[str, int]] = [(k, v) for k, v in compressed_sizes.items()
                if compressed_sizes[k] + uncompressed_sizes[k] <= MAX_SSTABLE_DATA_SIZE
                and (compressed_sizes[k] >= MIN_SSTABLE_DATA_SIZE or uncompressed_sizes[k] > MIN_SSTABLE_DATA_SIZE)]

        # Just in case the script is using too much RAM at this point.
        del compressed_sizes
        del uncompressed_sizes

        # Randomly pick some of the viable sstables for proessing, with probability weighted by size.
        choices: set[str] = set()
        data_in_samples = 0
        cum_weights = list(itertools.accumulate(v for k, v in viable))
        assert cum_weights[-1] > 1024 # Ensure progress
        # Note that we might choose the same file multiple times. We will log this choice each time,
        # but we will only download and test the file once.
        while data_in_samples < DATA_TO_SAMPLE:
            k, v = random.choices(viable, cum_weights=cum_weights)[0]
            data_in_samples += v
            logger.info(fr"Choosing s3://{BUCKET}/{k}-Data.db, size {v} B, total so far: {data_in_samples} B")
            choices.add(k)

        # Just in case the script is using too much RAM at this point.
        del cum_weights
        del viable

        # Collect the URLs of all sstable components for the chosen sstables.
        components: defaultdict[str, list[dict]] = defaultdict(list)
        component_regex = re.compile("^(.*)-.*[.].*$")
        for obj in list_objects(client, BUCKET, PREFIX):
            if (m := component_regex.fullmatch(obj["Key"])):
                sst = m.group(1)
                if sst in choices:
                    components[sst].append(obj)

        # Download and process each chosen sstable.
        workdir = "estimation_workdir"
        for sst, component_list in components.items():
            if os.path.exists(workdir):
                shutil.rmtree(workdir)
            os.makedirs(workdir)
            for obj in component_list:
                key = obj['Key']
                size = obj['Size']
                target = str(pathlib.Path(workdir)/pathlib.Path(obj['Key']).name)
                if key.endswith("Index.db"):
                    # `scylla sstable decompress` refuses to work if the Index.db doesn't exist, even
                    # though it's not actually used for anything.
                    # Let's create an empty file to shut it up without wasting space for downloading the real Index.
                    pathlib.Path(target).touch()
                    continue
                logger.info(fr"Downloading s3://{BUCKET}/{key} ({size} B) to {target}")
                client.download_file(BUCKET, obj['Key'], target)
            data_file = fr"{workdir}/{pathlib.Path(sst).name}-Data.db"
            logger.info(fr"Uncompressing {data_file}")
            try:
                subprocess.check_call([fr"{shlex.quote(SCYLLA)} sstable decompress {shlex.quote(data_file)}"], shell=True, stdout=sys.stderr)
            except:
                logger.error(fr"Failed to decompress {data_file}, skipping.")
                continue
            decompressed_name = fr"{data_file}.decompressed" 
            file_for_test = decompressed_name if os.path.exists(decompressed_name) else data_file
            logger.info(fr"Testing {file_for_test}")
            subprocess.check_call([fr"zstd -B4096 --train {shlex.quote(file_for_test)} -o dict 2>&1 | tr '\r' '\n'"], shell=True, stdout=sys.stderr)
            print(fr"s3://{BUCKET}/{sst}")
            before = subprocess.check_output([fr"zstd -B4096 -i0 -b3 {shlex.quote(file_for_test)} | tr '\r' '\n' | grep ',.*,'"], shell=True)
            print("nodict:", before)
            after = subprocess.check_output([fr"zstd -B4096 -i0 -b3 {shlex.quote(file_for_test)} -D dict | tr '\r' '\n' | grep ',.*,'"], shell=True)
            print("dict:", after)

    logger.info(fr"Done")

if __name__ == "__main__":
    main()
