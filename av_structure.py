import bagit
import os
import siplib
import argparse
import re
import logging
from lxml import etree
FORMAT = '%(asctime)-15s [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()


def id_transform(number):
    """
    Takes a variant of a UMA item number and returns a normalised version.
    """
    num_matches = re.findall(
        r'\d{4}[,_.-]\d{2,4}[,_.-]\d{1,5}', number)
    if len(num_matches) == 1:
        r = re.split(r'[,_.-]', num_matches[0])
        if len(r[1]) < 4:
            pre = 4-len(r[1])
            r[1] = '0'*pre+r[1]
        if len(r[2]) < 5:
            pre = 5-len(r[2])
            r[2] = '0'*pre+r[2]
        return('.'.join(r))


def get_rep(fname):
    name, ext = os.path.splitext(fname)
    if ext == '.mxf':
        if name.endswith('imx'):
            label = 'Mezzanine'
            type = 'Access'
        else:
            label = 'Preservation'
            type = 'Preservation'
    elif ext in ('.mp4', '.mp3'):
        label = 'Access'
        type = 'Access'
    else:
        label = 'Preservation'
        type = 'Preservation'
    return (label, type)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Split a SIP from a bagit package')
    parser.add_argument(
        'dir', metavar='i', type=str, help='bag')
    parser.add_argument(
        'out', metavar='o', type=str, help='output dir')
    parser.add_argument(
        '--separator', type=str,
        help='filename separator to split a filename on')
    parser.add_argument(
        '--identifier', type=str,
        help='filename separator to split a filename on')
    parser.add_argument(
        '--target', type=str,
        help='target folder for sip')
    parser.add_argument(
        '--security', type=str, default='open',
        help='security tag')
    parser.add_argument(
        '--metadata', type=str, default='open',
        help='path to a metadat file to be appended to top struct')

    args = parser.parse_args()
    ident = re.compile(r'\d{4}\.\d{4}\.\d{5}')
    bag = bagit.Bag(args.dir)
    os.chdir(bag.path)
    sip = siplib.Sip(os.path.join(args.out, args.identifier+'.zip'))
    base = sip.add_struct(args.identifier, args.target, security_tag=args.security)
    sip.add_identifier(base, args.identifier)
    for file, hash in bag.payload_entries().items():
        fname = os.path.split(file)[1]
        id = id_transform(file)
        if id == args.identifier:
            iname, ext = os.path.splitext(fname)
            if iname.endswith('imx'):
                iname = os.path.splitext(iname)[0]
            if iname not in sip.get_infobjs().keys():
                info = sip.add_info(iname, base, security_tag=args.security)
            else:
                info = sip.get_infobjs()[iname]
            c_object = sip.add_content(fname, info)
            label, type = get_rep(fname)
            sip.add_rep(label, info, type, [c_object])
            sip.add_gen(c_object, '', file.replace('\\', '/'))
            sip.add_bstream(file, hash)
    meta = etree.parse(args.metadata).getroot()
    sip.add_metadata(base, meta)
    sip.write_xip()
    sip.write_protocol(args.target, args.identfier)
    sip.close()
