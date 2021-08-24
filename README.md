# Preservica library and tools

Python library and command line scripts for building Preservica V6 Submission
Information packages (SIPs). Preservica 6 has a flexible data model but
few client side tools that take advantage of it. Use cases such as ingesting
files with pre-existing checksums and appending multiple content objects to
single representations requires building XIP metadata outside of Preservica's
existing tools. This package aims to do that in a way that's flexible and
scalable for anyone with basic Python scripting knowledge.

## Installation
Eventually I will package this properly. For now, clone and use within the repo
directory.

## Usage
xip_builder is the central library and consists of a single class for building XIP
based packages for Preservica. The Sip class inherits from
zipfile.ZipFile. On initialisation, this class creates an empty zipfile and a
root node for XIP metadata. Methods can then be used to build the SIP hierarchy,
add identifiers and metadata and write content to the zipfile in a structure
that Preservica can then interpret on ingest.
Note that you will have to build the various objects in hierarchical order in
order to build the SIP structure. Add object methods return the created object's
reference UUID, that can then be appended to subsequent objects. For example:

```
from pyservica.xip_builder import Sip
sip = Sip(sip_path, target)
struct_ref = sip.add_structobj(foldername, target)
asset_ref= sip.add_infobj(ident, struct_ref)
sip.add_manifestation(asset_ref, filepath, 'Preservation')
sip.add_manifestation(asset_ref, filepath, 'Access')
sip.serialise()
```

The serialise() method finally writes xml metadata and closes the zipfile
once the sip structure has been finalised.

API documentation is in the API directory.

This project is in very early stages and the API will likely change frequently.
