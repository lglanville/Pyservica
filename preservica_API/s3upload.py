import pathlib
from uuid import uuid4
import sys
import os
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor
import logging
import boto3
from boto3.s3.transfer import TransferConfig
MB = 1024 ** 2
GB = 1024 ** 3
logging.basicConfig(
    format=f'%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('S3upload')
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.ERROR)
logger.addHandler(ch)
CONFIG = TransferConfig(multipart_threshold=GB)


class ProgressTracker(object):
    def __init__(self):
        self._size = 0
        self._numfiles = 0
        self._seen_so_far = 0
        self.completed = 0
        self.failed = 0
        self._lock = threading.Lock()

    def trackfile(self, fpath):
        fpath = pathlib.Path(fpath)
        with self._lock:
            self._size += fpath.stat().st_size
            self._numfiles += 1

    def complete(self):
        with self._lock:
            self.completed += 1

    def fail(self):
        with self._lock:
            self.failed += 1

    def displaymessage(self):
        if self._size < GB:
            dsize = f"{round(self._size / MB, ndigits=2)}mb"
            dseen = f"{round(self._seen_so_far / MB, ndigits=2)}mb"
        else:
            dsize = f"{round(self._size / GB, ndigits=2)}gb"
            dseen = f"{round(self._seen_so_far / GB, ndigits=2)}gb"
        percentage = round((self._seen_so_far / self._size) * 100, ndigits=2)
        basemessage = f'\rUploaded {self.completed} of {self._numfiles} package(s)'
        if self.failed > 0:
            basemessage += f' ({self.failed} failed)'
        message = basemessage + f', {dseen} / {dsize} ({percentage}%)    '
        return message

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write(self.displaymessage())
            sys.stdout.flush()


TRACKER = ProgressTracker()


def get_client(bucketpath):
    """Get the thing that uploads files to bucketpath"""
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketpath)
    return bucket


def S3upload(file, bucketpath, delete_source=True, client=None):
    """Function for S3 upload with minimum Preservica required metadata.
    If using this method, ensure either the destination bucket is configured
    as a source for a workflow context with a destination folder, or that
    the package for upload contains XIP metadata specifying a destination
    folder. Simple packages uploaded via this method without a destination
    folder configured in the workflow context will fail at ingest.
    """
    if client is None:
        bucket = get_client(bucketpath)
    else:
        bucket = client
    fpath = pathlib.Path(file)
    key = str(uuid4())
    metadata = {"Metadata": {
        "key": key,
        "name": fpath.name,
        "size": str(round(fpath.stat().st_size/1024))}}
    with fpath.open('rb') as data:
        logger.info(f'Uploading {fpath} to {bucketpath}')
        try:
            TRACKER.trackfile(fpath)
            bucket.upload_fileobj(
                data, key,  ExtraArgs=metadata, Config=CONFIG,
                Callback=TRACKER)
            logger.info(f'Upload of {fpath} complete')
            TRACKER.complete()
        except boto3.exceptions.S3UploadFailedError as e:
            logger.exception(e)
            TRACKER.fail()
    if delete_source:
        try:
            fpath.unlink()
            logger.info(f'Removed source package {fpath}')
        except Exception as e:
            logger.error(f'Unable to delete source package {fpath}')
            logger.exception(e)


def _done_callback(future):
    try:
        future.result()
    except Exception as e:
        logger.exception(e)


def bulks3upload(directory, bucketpath, delete_source=True):
    """Bulk upload method. Threads are capped at 5, beyond this AWS
    has problems"""
    bucket = get_client(bucketpath)
    with ThreadPoolExecutor(5) as ex:
        for file in os.scandir(directory):
            if file.name.endswith('zip'):
                f = ex.submit(
                    S3upload, *(file.path, bucketpath),
                    **{'client': bucket, 'delete_source': delete_source})
                f.add_done_callback(_done_callback)


def configlogfile(logfile):
    """Configures a rotating log file with INFO debug level."""
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(
        logfile, 'a', maxBytes=MB, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload some SIPs to an S3 bucket with Preservica required'
        ' metadata')
    parser.add_argument(
        'i', metavar='<input package or directory>', type=str,
        help='Path for package or base directory if using --bulk option')
    parser.add_argument(
        'bucket', type=str, help='Path to S3 bucket')
    parser.add_argument(
        '--bulk', '-b', action='store_true',
        help='Upload all .zip packages in directory')
    parser.add_argument(
        '--deletesource', '-d', action='store_true',
        help='Delete source packages on successful upload')
    parser.add_argument(
        '--logfile', '-l',
        help='Path to a log file. If omitted, will log to console')
    args = parser.parse_args()

    if args.logfile is not None:
        configlogfile(args.logfile)

    if args.bulk:
        bulks3upload(args.i, args.bucket, delete_source=args.deletesource)
    else:
        S3upload(args.i, args.bucket, delete_source=args.deletesource)
