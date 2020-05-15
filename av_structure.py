import bagit
import os
import siplib
import argparse
import re
import logging
import pathlib
from lxml import etree
from API.s3upload import S3upload
FORMAT = '%(asctime)-15s [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()


def id_transform(filename):
    """Takes a filename containing a variant of a UMA item number and returns a
    normalised version.
    """
    num_matches = re.findall(
        r'\d{4}[,_.-]\d{2,4}[,_.-]\d{1,5}', filename)
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
    """Takes a filepath and returns the appropriate label and type for the
    representation element"""
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


def video(metadata, bagdirs, outdir, parent, security='open', write=True):
    """Main fuction that builds the SIP with the provided metadata"""
    meta = etree.parse(metadata).getroot()
    title = meta.find('mods:titleInfo/mods:title', namespaces=meta.nsmap).text
    identifier = meta.find('mods:identifier[@type="UMA"]', namespaces=meta.nsmap).text
    sipfile = os.path.join(outdir, identifier+'.zip')
    sip = siplib.Sip(sipfile, parent)
    base = sip.add_structobj(title, parent, security_tag=args.security)
    sip.add_identifier(base, identifier)
    sip.add_metadata(base, meta)
    for bagdir in bagdirs:
        os.chdir(bagdir)
        bag = bagit.Bag(bagdir)
        for file, hash in bag.payload_entries().items():
            fpath = pathlib.Path(file)
            id = id_transform(fpath.name)
            if id == identifier:
                if fpath.suffix == '.xml':
                    iname = identifier+' SAMMA xml'
                elif fpath.suffix.lower() in ['.mxf', '.mp4', '.iso']:
                    iname = identifier
                else:
                    iname = fpath.stem
                    if iname.endswith('imx'):
                        iname = os.path.splitext(iname)[0]
                if iname not in sip.get_infobjs().values():
                    info_ref = sip.add_infobj(iname, base, security_tag=security)
                else:
                    info_ref = [key for key, val in sip.get_infobjs().items() if val == iname][0]
                c_object = sip.add_contobj(fpath.name, info_ref, security_tag=security)
                label, type = get_rep(fpath.name)
                sip.add_representation(label, info_ref, [c_object], type=type)
                if label in ('Mezzanine', 'Access'):
                    sip.add_generation(c_object, '', [fpath], orig='false')
                else:
                    sip.add_generation(c_object, '', [fpath])
                norm_hash = {alg.upper(): val for alg, val in hash.items()}
                sip.add_bitstream(file, norm_hash, write=write)
    sip.close()
    return sipfile


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Split a SIP from a bagit package')
    parser.add_argument(
        '--dirs', metavar='i', nargs='+', type=str, help='Input bags')
    parser.add_argument(
        '--out', metavar='o', type=str, help='output directory for SIP')
    parser.add_argument(
        '--target', type=str,
        help='target folder in Preservica for sip')
    parser.add_argument(
        '--security', type=str, default='open',
        help='security tag')
    parser.add_argument(
        '--metadata', type=str,
        help='path to a metadata file to be appended to top struct')
    parser.add_argument(
        '--dummy', action='store_false',
        help="Don't write bitstreams, just package structure (for evaluation)")
    parser.add_argument(
        '--s3',
        help="path to s3 bucket for upload")

    args = parser.parse_args()
    sipfile = video(
        args.metadata, args.dirs, args.out, args.target,
        security=args.security, write=args.dummy)
    if args.s3 is not None:
        S3upload(sipfile, args.s3)
