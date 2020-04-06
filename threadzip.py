import threading
import zipfile
import os
import time
import concurrent.futures
import sys

filelist = []
for root, _, files in os.walk(sys.argv[1]):
    for file in files:
        filelist.append(os.path.join(root, file))


def zip_files(zipname, files):
    z = zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_BZIP2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(z.write, files)
    z.close()

zip = r"C:\Users\lglanville\zip.zip"
tzip = r"C:\Users\lglanville\tzip.zip"

start_time = time.time()
print(filelist)
zip_files(tzip, filelist)
duration = time.time() - start_time
print(duration)
