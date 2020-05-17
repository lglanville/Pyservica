import pathlib
from uuid import uuid4
import sys
import threading
import boto3
from boto3.s3.transfer import TransferConfig


def S3upload(file, bucketpath, destination=None):
    """Function for S3 upload with minimum Preservica required metadata.
    Destination is not required for XIP based packages as the destination is
    set within the XIP metadata
    """
    fpath = pathlib.Path(file)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketpath)
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=GB)

    key = str(uuid4())
    metadata = {"Metadata": {
        "key": key,
        "name": fpath.name,
        "size": str(round(fpath.stat().st_size/1024))}}
    if destination is not None:
        metadata["Metadata"]["structuralobjectreference"] = destination
    with fpath.open('rb') as data:
        bucket.upload_fileobj(
            data, key,  ExtraArgs=metadata, Config=config,
            Callback=ProgressPercentage(fpath))


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename.name
        self._size = float(filename.stat().st_size)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                f'\rUploading {self._filename}, {self._seen_so_far} / {self._size} ({percentage}%)')
            sys.stdout.flush()


if __name__ == '__main__':
    S3upload(sys.argv[1], sys.argv[2], sys.argv[3])
