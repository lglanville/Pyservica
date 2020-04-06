import siplib
import bagit
import os
import sys
import pathlib

bag = bagit.Bag(sys.argv[1])
os.chdir(bag.path)
sip = siplib.Sip(os.path.join(sys.argv[2], os.path.split(sys.argv[1])[1]))
parent = "e9c10eef-9365-4b11-be58-2975cb1cc20b"
for root, dirs, files in os.walk('data/objects'):
    if root == 'data/objects':
        cdir = sip.add_struct(bag.info['identifier'], parent_ref=parent)
        sip.add_identifier(cdir, bag.info['identifier'])
    else:
        cdir = sip.add_struct(os.path.split(root)[1], parent_ref=cdir)
    for file in files:
        fpath = pathlib.Path(root) / file
        hash = [hash for file, hash in bag.payload_entries().items() if pathlib.Path(file) == fpath]
        if len(hash) == 1:
            norm_hash = {alg.upper(): val for alg, val in hash[0].items()}
            sip.add_tree(parent, fpath, checksum=norm_hash)
        else:
            raise ValueError('Too many hashes')

sip.write_xip()
sip.write_protocol(parent, os.path.split(sys.argv[1])[1])
sip.close()
