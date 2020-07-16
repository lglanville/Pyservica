import bagit
import os
import argparse
import re
import logging
import pathlib
from lxml import etree
from siplib import Sip
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
    else:
        match = re.search(
            r'GreerG_(\d{1,3})?[AB]?.', filename)
        if match is not None:
            i = match.groups()[0]
            return '2014.0040.'+'0'*(5-len(i))+i


def get_rep(filename):
    """Takes a filepath and returns the appropriate label and type for the
    representation element"""
    name, ext = os.path.splitext(filename)
    if ext in ('.wav', '.docx', '.oma', '.mxf', '.iso'):
        if 'original' in filename:
            label = 'Preservation-2 original'
            type = 'Preservation'
        elif 'remastered' in filename:
            label = 'Preservation-1 remastered'
            type = 'Preservation'
        elif name.endswith('imx'):
            label = 'Mezzanine'
            type = 'Access'
        else:
            label = 'Preservation'
            type = 'Preservation'
    elif ext in ('.pdf', '.mp3', '.mp4'):
        label = 'Access'
        type = 'Access'
    else:
        label = 'Preservation'
        type = 'Preservation'
    return (label, type)


def get_iname(filename, ident):
    """Takes a filename and identifier and returns an appropriate name for the
    intellectual object"""
    if filename.suffix in ('.wav', '.oma', '.mp3', '.mp4', '.mxf', '.iso'):
        if filename.stem.upper().endswith('A'):
            return ident+' side A'
        elif filename.stem.upper().endswith('B'):
            return ident+' side B'
        else:
            return ident
    elif filename.suffix in ('.pdf', '.docx'):
        return ident+' timecoded summary'
    elif filename.suffix == '.xml':
        return ident+' XML'
    else:
        return pathlib.Path(filename).stem


def bag_scan(bagdir, identifier, sip, base, security='open', write=True):
    """traverses a bagit package searching for files that match the provided
    identifier, then writing them to the SIP with the bagit checksums"""
    os.chdir(bagdir)
    bag = bagit.Bag(bagdir)
    for file, checksums in bag.payload_entries().items():
        fpath = pathlib.Path(file)
        id = id_transform(fpath.name)
        if id == identifier:
            checksums = {alg.upper(): val for alg, val in checksums.items()}
            add_asset(fpath, checksums, identifier, sip, base, security='open', write=True)


def dir_scan(dir, identifier, sip, base, security='open', write=True):
    """recursively traverses a directory searching for files that match the
    provided identifier, then writing them to the SIP with generated checksums.
    """
    os.chdir(dir)
    for root, _, files in os.walk(dir):
        for file in files:
            fpath = pathlib.Path(root, file).relative_to(pathlib.Path().cwd())
            id = id_transform(fpath.name)
            if id == identifier:
                checksums = Sip.hash_file(fpath)
                add_asset(fpath, checksums, identifier, sip, base, security='open', write=True)


def add_asset(fpath, checksums, identifier, sip, base, security='open', write=True):
    if not find_dupe(checksums, sip):
        iname = get_iname(fpath, identifier)
        if iname not in sip.get_infobjs().values():
            info_ref = sip.add_infobj(iname, base, security_tag=security)
        else:
            info_ref = [key for key, val in sip.get_infobjs().items() if val == iname][0]
        c_object = sip.add_contobj(fpath.name, info_ref, security_tag=security)
        label, type = get_rep(fpath.as_posix())
        sip.add_representation(label, info_ref, [c_object], type=type)
        if label == 'Preservation original':
            sip.add_generation(c_object, '', [fpath], orig='false', active='false')
        elif label == 'Access':
            sip.add_generation(c_object, '', [fpath], orig='false', active='true')
        else:
            sip.add_generation(c_object, '', [fpath])
        sip.add_bitstream(fpath, checksums, arcname=fpath.name, write=write)


def find_dupe(new_hash, sip):
    """checks if a file has already been written to the SIP"""
    for file, hashes in sip.get_checksums().items():
        for alg, hash in hashes.items():
            if alg in new_hash.keys():
                if hash == new_hash[alg]:
                    return True


def build_sip(metadata, dirs, outdir, parent, security='open', write=True):
    """Main fuction that builds the SIP with the provided metadata"""
    meta = etree.parse(metadata).getroot()
    title = meta.find('mods:titleInfo/mods:title', namespaces=meta.nsmap).text
    identifier = meta.find('mods:identifier[@type="UMA"]', namespaces=meta.nsmap).text
    sipfile = os.path.join(outdir, identifier+'.zip')
    sip = Sip(sipfile, parent)
    base = sip.add_structobj(title, parent, security_tag=security)
    sip.add_identifier(base, identifier)
    sip.add_metadata(base, meta)
    for dir in dirs:
        if 'bagit.txt' in os.listdir(dir):
            bag_scan(dir, identifier, sip, base, security=security, write=write)
        else:
            dir_scan(dir, identifier, sip, base, security=security, write=write)
    sip.serialise()
    sip.close()
    return sipfile


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build a sip relating to a single recording')
    parser.add_argument(
        '--dirs', metavar='i', nargs='+', type=str,
        help='input bags and directories')
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
    sipfile = build_sip(
        args.metadata, args.dirs, args.out, args.target,
        security=args.security, write=args.dummy)
    if args.s3 is not None:
        S3upload(sipfile, args.s3)
