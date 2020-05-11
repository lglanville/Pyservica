import siplib
import bagit
import os
import sys
import pathlib


def main(bagdir, outdir, parent_ref):
    bag = bagit.Bag(bagdir)
    os.chdir(bag.path)
    bag_path = pathlib.Path(bagdir)
    sip_path = pathlib.Path(outdir, bag_path.name+'.zip')
    sip = siplib.Sip(sip_path, parent_ref)
    for root, dirs, files in os.walk('data/objects'):
        if root == 'data/objects':
            parent_ref = sip.add_structobj(
                bag.info['identifier'], parent_ref=parent_ref)
            sip.add_identifier(parent_ref, bag.info['identifier'])
        else:
            parent_ref = sip.add_structobj(
                os.path.split(root)[1], parent_ref=parent_ref)
        for file in files:
            fpath = pathlib.Path(root) / file
            hash = [hash for file, hash in bag.payload_entries().items() if pathlib.Path(file) == fpath]
            if len(hash) == 1:
                norm_hash = {alg.upper(): val for alg, val in hash[0].items()}
                sip.add_tree(parent_ref, fpath, checksum=norm_hash)
            else:
                raise ValueError('Too many hashes')
    sip.serialise()
    sip.close()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
