import sys
import os
import re
from preservicaAPI import get_session, logger

"""Simple bulk upload script for use with UMA SIPs. SIPs must be zip
files with the item number in the filename"""


os.chdir(sys.argv[1])
with get_session() as sesh:
    for file in os.listdir(sys.argv[1]):
        if file.endswith('zip'):
            m = re.match(r'\d{4}.\d{4}', file)
            if m is not None:
                refs = sesh.get_refs(m.group())
                if len(refs['structural-objects']) == 1:
                    parent = refs['structural-objects'][0]
                    sesh.upload(file, parent)
                elif len(refs['structural-objects']) > 1:
                    logger.error(
                        'Multiple structural objects with identifier',
                        m.group())
                else:
                    logger.error('No structural objects with identifier', m.group())
