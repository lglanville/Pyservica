# Preservica API library

Python module that wraps requests.session for working with the Preservica API.

## Usage
The core library including the preservica_session class is in the file
preservicaAPI.py. The preservica_session class can be instantiated directly,
however for ease of use it can be created using the get_session function.
This reads credentials from a config.json file in your home directory at
.preservica/config.json. The write_config function can create this file for you.
On the command line:
```
python API.py  --config [host] [tenant] [username] [password]
```
The preservicaAPI.py file also contains a simple option for package upload via
the API. Note this method is only suitable for packages <= 10gb.
```
python API.py  --upload [package file] [folder ref]
```
The s3upload.py file is a simple script/function for uploading a package to
a configured AWS S3 source bucket. Packages are uploaded with required S3 tags
for Preservica to detect and process a valid package. This script requires
AWS credentials to be configured and the boto3 library installed.
