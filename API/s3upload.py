import pathlib
from uuid import uuid4
import sys
import threading
import boto3
from boto3.s3.transfer import TransferConfig


def S3upload(file, destination, bucketpath):
    fpath = pathlib.Path(file)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketpath)
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=GB)

    key = str(uuid4())
    metadata = {"Metadata": {
        "structuralobjectreference": destination,
        "key": key,
        "name": fpath.name,
        "size": str(round(fpath.stat().st_size/1024))}}
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
