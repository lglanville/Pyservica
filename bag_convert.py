from pyservica import Sip
import bagit
from pathlib import Path
import sys
import os


def main(bagdir, outdir, parent_ref, security_tag='UMA_restricted'):
    """traverses a bagit package with an object directory, converting it to a
    V6 SIP using existing checksums
    """
    bag = bagit.Bag(bagdir)
    bag_path = Path(bagdir)
    sip_path = Path(outdir, bag_path.name+'.zip')
    objects = bag_path / 'data/objects'
    sip = Sip(sip_path, parent_ref)
    for root, dirs, files in os.walk(objects):
        root = Path(root)
        if root == objects:
            parent_ref = sip.add_structobj(
                bag.info['identifier'], parent_ref=parent_ref, security_tag=security_tag)
            sip.add_identifier(parent_ref, bag.info['identifier'])
        else:
            parent_ref = sip.add_structobj(
                root.name, parent_ref=parent_ref, security_tag=security_tag)
        for file in files:
            fpath = Path(root) / file
            relpath = fpath.relative_to(bag_path)
            hash = [hash for file, hash in bag.payload_entries().items() if Path(file) == fpath]
            if len(hash) == 1:
                norm_hash = {alg.upper(): val for alg, val in hash[0].items()}
                sip.add_asset_tree(parent_ref, fpath, arcname=rel_path, checksum=norm_hash, security_tag=security_tag)
            else:
                raise ValueError('Too many hashes')
    sip.serialise()
    sip.close()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
