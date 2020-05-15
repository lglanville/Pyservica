# Preservica library and tools

Python library and command line scripts for building Preservica V6 Submission
Information packages (SIPs). Preservica 6 has a flexible data model but
few tools that can take advantage of it. Use cases such as ingesting
files with pre-existing checksums and appending multiple content objects to
single representations require building XIP metadata outside of Preservica's
existing tools.

## Installation
Eventually I will package this properly. For now, clone and use within the repo directory.

##usage
siplib.py is the central library. The Sip class inherits from
zipfile.ZipFile. On initilisation, this class creates an empty zipfile. Methods
then write content to the zipfile in a structure that Preservica can
then interpret on ingest. The serialise() method finally writes xml metadata
once the sip structure has been finalised.

This project is in very early stages and the API will likely change frequently.
