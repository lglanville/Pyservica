# Preservica API library

Python library that wraps requests.session for working with the Preservica API.

## Installation
Eventually I will package this properly. For now, clone and use within the repo directory.

## Usage
The core library including the preservica_session class is in the file
preservicaAPI.py. The preservica_session class can be instantiated directly,
however for ease of use it can be created using the get_session function.
This reads credentials from a config.json file in your home directory at
.preservica/config.json. The write_config function can create this file for you.
On the command line:
```
python preservicaAPI.py  --config [host] [tenant] [username] [password]
```
The preservicaAPI.py file also contains a simple option for package upload.
```
python preservicaAPI.py  --upload [package file] [folder ref]
```
