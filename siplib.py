from lxml import etree
import sys
from uuid import uuid4
from datetime import datetime
import os
import hashlib
import argparse
import zipfile
import getpass
import pathlib
from fnmatch import fnmatch
import logging
FORMAT = '%(asctime)-15s [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('siplog')
logger.setLevel(logging.INFO)


class Sip(zipfile.ZipFile):
    def __init__(self, fpath):
        if os.path.exists(fpath):
            os.remove(fpath)
            print("Haven't implemented this bit yet")
        #init an empty sip
        super(Sip, self).__init__(fpath, 'w')
        self.sipref = str(uuid4())
        self.xip = etree.Element(
            'XIP',
            nsmap={None: "http://preservica.com/XIP/v6.0"})
        self.content = os.path.join(self.sipref, 'content')
        self.filecount = 0
        self.filesize = 0

    def get_structs(self):
        structs = {}
        for e in self.xip.findall('StructuralObject', namespaces=self.xip.nsmap):
            title = e.find('Title', namespaces=self.xip.nsmap).text
            ref = e.find('Ref', namespaces=self.xip.nsmap).text
            structs[title] = ref
        return structs

    def get_infobjs(self):
        infobjs = {}
        for e in self.xip.findall('InformationObject', namespaces=self.xip.nsmap):
            title = e.find('Title', namespaces=self.xip.nsmap).text
            ref = e.find('Ref', namespaces=self.xip.nsmap).text
            infobjs[title] = ref
        return infobjs

    def add_structobj(self, title, parent_ref=None, security_tag='open'):
        """
        Structural objects make up the hierarchy within your archive. They can
        contain other structural objects, building up a tree structure, and
        also information objects. Parent is the uuid of the destination folder
        in Preservica.
        """
        ref = str(uuid4())
        logger.info(f'Adding StructuralObject {title} {ref}')
        sobj = etree.SubElement(self.xip, 'StructuralObject')
        etree.SubElement(sobj, 'Ref').text = ref
        etree.SubElement(sobj, 'Title').text = title
        etree.SubElement(sobj, 'SecurityTag').text = security_tag
        if parent_ref:
            etree.SubElement(sobj, 'Parent').text = parent_ref
        return ref

    def add_infobj(self, title, folder_ref, security_tag='open'):
        """
        A logically atomic piece of information, for example a picture or an
        email. Assets are the entities which you will generally interact with,
        for example by rendering or downloading them. An asset can’t contain
        other assets although it has substructure (see below), it is
        self contained and not hierarchical.
        folder_ref is the uuid of the containing structural object.
        """
        ref = str(uuid4())
        logger.info(f'Adding InformationObject {title} {ref}')
        sobj = etree.SubElement(self.xip, 'InformationObject')
        etree.SubElement(sobj, 'Ref').text = ref
        etree.SubElement(sobj, 'Title').text = title
        etree.SubElement(sobj, 'SecurityTag').text = security_tag
        etree.SubElement(sobj, 'Parent').text = folder_ref
        return ref

    def add_representation(self, name, info_ref, type, c_objects):
        """
        A way of viewing the information within an asset. The most common
        example of different representations would be a preservation copy and
        an access copy, for example a video in its initial format for
        preservation purposes, and a downscaled web-ready MP4 for access
        purposes. Information objects will always have at least one
        representation (the preservation representation).
        """
        rep = etree.SubElement(self.xip, 'Representation')
        logger.info(f'Adding Representation {name} to {info_ref}')
        etree.SubElement(rep, 'InformationObject').text = info_ref
        etree.SubElement(rep, 'Name').text = name
        etree.SubElement(rep, 'Type').text = type
        con = etree.SubElement(rep, 'ContentObjects')
        for c_object in c_objects:
            etree.SubElement(con, 'ContentObject').text = c_object

    def add_contobj(self, fname, info_ref, security_tag='open'):
        """
        A logically atomic piece of content, for example an attachment or an
        email. Links to multiple bitstreams
        """
        ref = str(uuid4())
        logger.info(f'Adding ContentObject {fname} {ref} to {info_ref}')
        content = etree.SubElement(self.xip, 'ContentObject')
        etree.SubElement(content, 'Ref').text = ref
        etree.SubElement(content, 'Title').text = fname
        etree.SubElement(content, 'SecurityTag').text = security_tag
        etree.SubElement(content, 'Parent').text = info_ref
        return ref

    def add_generation(self, contobj_ref, label, bitstreams):
        """
        Content objects contain generations, as a sequence of dated views of
        the content in different formats. When content is preserved, a new
        generation will be created, with the content in a less at-risk format.
        Only the most recent generation of content will be used by default
        (e.g. for retrieving content to render).
        """
        gen = etree.SubElement(
            self.xip, 'Generation', original="true", active="true")
        logger.info(f'Adding Generation {label} to {contobj_ref}')
        etree.SubElement(gen, 'ContentObject').text = contobj_ref
        etree.SubElement(gen, 'Label').text = label
        etree.SubElement(gen, 'EffectiveDate').text = datetime.now().isoformat()
        b = etree.SubElement(gen, 'Bitstreams')
        for bitstream in bitstreams:
            fpath = pathlib.Path(bitstream)
            etree.SubElement(b, 'Bitstream').text = fpath.as_posix()
        etree.SubElement(gen, 'Formats')
        etree.SubElement(gen, 'Properties')

    def add_bitstream(self, fpath, checksums):
        """
        The physical content, as stored on a storage adapter. In almost all
        cases, a generation of a CO will have only one bitstream; exceptions
        would include multi-part container files, or data formats where the
        data header and content are in separate physical files (which is rare).
        Links to a content object.
        """
        fpath = pathlib.Path(fpath)
        logger.info(f'Writing {fpath} to package')
        self.write(fpath, self.content / fpath)
        self.filecount += 1
        size = fpath.stat().st_size
        self.filesize += size
        bstream = etree.SubElement(self.xip, 'Bitstream')
        path, name = os.path.split(fpath)
        posix_path = fpath.parent.as_posix()
        etree.SubElement(bstream, 'Filename').text = name
        etree.SubElement(bstream, 'FileSize').text = str(size)
        etree.SubElement(bstream, 'PhysicalLocation').text = posix_path
        fixities = etree.SubElement(bstream, 'Fixities')
        for alg, hash in checksums.items():
            fixity = etree.SubElement(fixities, 'Fixity')
            etree.SubElement(fixity, 'FixityAlgorithmRef').text = alg
            etree.SubElement(fixity, 'FixityValue').text = hash

    def add_tree(self, parent_ref, fpath, security_tag='open', checksum=None):
        """
        Simple method for adding an InformationObject > ContentObject >
        Representation > Generation > Bitstream hierarchy where there's a 1:1
        relationship in the hierarchy.
        """
        fpath = pathlib.Path(fpath)
        i = self.add_infobj(parent_ref, fpath.stem, security_tag=security_tag)
        c = self.add_contobj(fpath.name, i, security_tag=security_tag)
        self.add_representation('Preservation-1', i, 'Preservation', [c])
        self.add_generation(c, '', [fpath])
        if checksum is None:
            checksum = sha256sum(fpath)
        self.add_bitstream(fpath, checksum)

    def write_protocol(self, parent, name):
        prot = etree.Element(
            'protocol',
            nsmap={None: "http://www.tessella.com/xipcreateprotocol/v1"})
        logger.info(f'Writing protocol')
        etree.SubElement(prot, 'dateCreated').text = datetime.now().isoformat()
        etree.SubElement(prot, 'size').text = str(self.filecount)
        etree.SubElement(prot, 'files').text = str(self.filesize)
        etree.SubElement(prot, 'submissionName').text = name
        etree.SubElement(prot, 'catalogueName').text = name
        etree.SubElement(prot, 'localAIP').text = self.sipref
        etree.SubElement(prot, 'globalAIP').text = parent
        etree.SubElement(prot, 'createdBy').text = getpass.getuser()
        tree = etree.ElementTree(prot)
        self.writestr(self.sipref+'.protocol', etree.tostring(
            tree, pretty_print=True, encoding="UTF-8",
            xml_declaration=True, standalone=True))

    def write_xip(self):
        logger.info(f'Writing XIP')
        tree = etree.ElementTree(self.xip)
        self.writestr(
            os.path.join(self.sipref, 'metadata.xml'),
            etree.tostring(
                tree, pretty_print=True, encoding="UTF-8",
                xml_declaration=True, standalone=True))

    def add_identifier(self, targetref, value, type='UMA'):
        logger.info(f'Adding Identifier {type} {value} to {targetref}')
        ident = etree.SubElement(self.xip, 'Identifier')
        etree.SubElement(ident, 'Type').text = type
        etree.SubElement(ident, 'Value').text = value
        etree.SubElement(ident, 'Entity').text = targetref

    def add_metadata(self, targetref, fragment):
        '''metadata fragment can be attached to
        StructuralObjects or InformationObjects'''
        ref = str(uuid4())
        nspace = fragment.tag.split('}')[0].strip('{')
        logger.info(f'Adding Metadata {nspace} to {targetref}')
        metadata = etree.SubElement(self.xip, 'Metadata', schemaUri=nspace)
        etree.SubElement(metadata, 'Ref').text = ref
        etree.SubElement(metadata, 'Entity').text = targetref
        content = etree.SubElement(metadata, 'Content')
        content.append(fragment)
        return ref


def sha256sum(filename):
    hash = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(128 * hash.block_size), b""):
            hash.update(chunk)
    return {'SHA256': hash.hexdigest()}


def suffsplit(fname, separator):
    name, ext = os.path.splitext(fname)
    parts = name.split(separator)
    if parts[0] == '':
        pref = parts[1]
        suff = parts[0]
    elif len(parts) == 1:
        pref = parts[0]
        suff = ''
    else:
        pref = separator.join(parts[:len(parts)-1])
        suff = parts[-1]
    return (pref, suff, ext)


def get_hash(hashfile):
    with open(hashfile) as f:
        alg = hashfile.split('.')[-1]
        hash = f.read()
    return {alg: hash}


def build_filemap(basedir, multi=False, sep=None):
    obj = {'files': 0, 'sizes': 0, 'metadata': [], 'filemap': {}}
    hashfiles = []
    for root, _, files in os.walk(sys.argv[1]):
        for file in files:
            relpath = os.path.relpath(os.path.join(root, file))
            if file.endswith(('sha256', 'sha512', 'sha1', 'md5')):
                hashfiles.append(relpath)
            elif file.endswith('.metadata'):
                obj['metadata'].append(relpath)
            else:
                obj['files'] += 1
                obj['sizes'] += os.path.getsize(relpath)
                if sep is not None:
                    pref, suff, ext = suffsplit(file, sep)
                else:
                    pref, ext = os.path.splitext(file)
                label = 'Preservation'
                if multi:
                    if ext.lower() != ('.wav', '.tif', '.mxf'):
                        label = 'Access'
                entry = {'path': relpath, 'label': label, 'hashes': {}}
                if pref in obj['filemap'].keys():
                    obj['filemap'][pref].append(entry)
                else:
                    obj['filemap'][pref] = [entry]
    for file in hashfiles:
        alg = file.split('.')[-1].upper()
        with open(file, newline='') as f:
            for line in f.readlines():
                line = line.strip().split(maxsplit=1)
                if len(line) == 2:
                    print(line)
                    hash, file = line
                    for pref, bstreams in obj['filemap'].items():
                        for bstream in bstreams:
                            print(bstream)
                            if fnmatch(bstream['path'], file):
                                entry['hashes'][alg] = hash
    return obj


def main(basedir, outdir, parent=None, sep=None, multi=False):
    sipname = os.path.split(basedir)[1]
    sip = Sip(os.path.join(outdir, sipname+'.zip'))
    os.chdir(basedir)
    obj = build_filemap(sys.argv[1], sep=sep, multi=multi)
    base = sip.add_structobj(sipname)
    for asset, entries in obj['filemap'].items():
        inf_obj = sip.add_infobj(asset, base)
        for entry in entries:
            con_obj = sip.add_contobj(os.path.split(entry['path'])[1], inf_obj)
            sip.add_representation(os.path.split(entry['path'])[0], inf_obj, entry['label'], [con_obj])
            sip.add_generation(con_obj, os.path.split(entry['path'])[0], [entry['path']])
            if entry['hashes'] == {}:
                entry['hashes'].update(sha256sum(entry['path']))
            sip.add_bitstream(entry['path'], entry['hashes'])
    for meta in obj['metadata']:
        e = etree.parse(meta)
        fragment = e.getroot()
        sip.add_metadata(base, fragment)
    sip.write_protocol(parent, sipname)
    sip.write_xip()
    sip.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build a SIP from a directory')
    parser.add_argument(
        'dir', metavar='i', type=str, help='base directory for a SIP')
    parser.add_argument(
        'out', metavar='o', type=str, help='base directory for a SIP')
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
        '--security', type=str,
        help='security tag')

    args = parser.parse_args()
    main(args.dir, args.out, parent=args.target, sep=args.separator)
