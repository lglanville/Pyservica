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
import logging
from io import BytesIO
FORMAT = '%(asctime)-15s [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('siplog')
logger.setLevel(logging.INFO)


class Sip(zipfile.ZipFile):
    def __init__(self, fpath):
        if os.path.exists(fpath):
            logger.info(f'Opening existing SIP at {fpath}')
            super(Sip, self).__init__(fpath, 'a')
            for file in self.filelist:
                if file.filename.endswith('.protocol'):
                    prot = file.filename
                    self.sipref = prot.split('.')[0]
                if file.filename.endswith('metadata.xml'):
                    self.xip = etree.parse(BytesIO(self.read(file.filename))).getroot()
            self.protocol = etree.parse(BytesIO(self.read(prot)))
            self.content = os.path.join(self.sipref, 'content')
        else:
            logger.info(f'Creating new SIP at {fpath}')
            super(Sip, self).__init__(
                fpath, 'w', compression=zipfile.ZIP_DEFLATED)
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
        for e in self.xip.findall('InformationObject'):
            title = e.find('Title').text
            ref = e.find('Ref').text
            infobjs[ref] = title
        return infobjs

    def get_checksums(self):
        sums = {}
        for elem in self.xip.findall('Bitstream'):
            name = elem.find('Filename').text
            sums[name] = {}
            for fixity in elem.findall('Fixities/Fixity'):
                alg = fixity.find('FixityAlgorithmRef').text
                hash = fixity.find('FixityValue').text
                sums[name][alg] = hash
        return sums

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
        if parent_ref is not None:
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

    def add_representation(self, name, info_ref, c_objects, type='Preservation'):
        """
        A way of viewing the information within an asset. The most common
        example of different representations would be a preservation copy and
        an access copy, for example a video in its initial format for
        preservation purposes, and a downscaled web-ready MP4 for access
        purposes. Information objects will always have at least one
        representation (the preservation representation).
        Representation names usually follow convention of type+ascending
        number, ie Preservation-1, but can be descriptive.
        """
        if type not in ('Access', 'Preservation'):
            raise ValueError(
                'Representation type must be Access or Preservation')
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
        email.
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

    def add_bitstream(self, fpath, checksums, write=True):
        """
        The physical content, as stored on a storage adapter. In almost all
        cases, a generation of a CO will have only one bitstream; exceptions
        would include multi-part container files, or data formats where the
        data header and content are in separate physical files (which is rare).
        Links to a content object.
        """
        fpath = pathlib.Path(fpath)
        if fpath.is_absolute():
            raise ValueError('Bitstream paths must be relative:', fpath)
        logger.info(f'Writing {fpath} to package')
        if write:
            self.write(fpath, self.content / fpath)
        self.filecount += 1
        size = fpath.stat().st_size
        self.filesize += size
        bstream = etree.SubElement(self.xip, 'Bitstream')
        path, name = os.path.split(fpath)
        posix_path = fpath.parent.as_posix()
        if posix_path == '.':
            posix_path = ''
        etree.SubElement(bstream, 'Filename').text = name
        etree.SubElement(bstream, 'FileSize').text = str(size)
        etree.SubElement(bstream, 'PhysicalLocation').text = posix_path
        fixities = etree.SubElement(bstream, 'Fixities')
        for alg, hash in checksums.items():
            if alg not in ['MD5', 'SHA1', 'SHA256', 'SHA512']:
                raise ValueError('Unsupported algorithm:', alg)
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
        self.add_representation('Preservation-1', i, [c])
        self.add_generation(c, '', [fpath])
        if checksum is None:
            checksum = sha256sum(fpath)
        self.add_bitstream(fpath, checksum)

    def write_protocol(self, parent_ref, name):
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
        etree.SubElement(prot, 'globalAIP').text = parent_ref
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

    def serialise(self, parent_ref, name):
        self.write_xip()
        self.write_protocol(parent_ref, name)
        self.close()

    def add_identifier(self, targetref, value, type='code'):
        """
        Identifiers can be attached to StructuralObjects,
        InformationObjects or ContentObjects.
        """
        logger.info(f'Adding Identifier {type} {value} to {targetref}')
        ident = etree.SubElement(self.xip, 'Identifier')
        etree.SubElement(ident, 'Type').text = type
        etree.SubElement(ident, 'Value').text = value
        etree.SubElement(ident, 'Entity').text = targetref

    def add_metadata(self, targetref, fragment):
        """
        Metadata fragments can be attached to StructuralObjects,
        InformationObjects or ContentObjects. fragment is the root element of
        an xml tree.
        """
        ref = str(uuid4())
        nspace = fragment.tag.split('}')[0].strip('{')
        logger.info(f'Adding Metadata {nspace} to {targetref}')
        metadata = etree.SubElement(self.xip, 'Metadata', schemaUri=nspace)
        etree.SubElement(metadata, 'Ref').text = ref
        etree.SubElement(metadata, 'Entity').text = targetref
        content = etree.SubElement(metadata, 'Content')
        content.append(fragment)
        return ref

    def add_extendedxip(self, targetref, earliest, latest, surrogate=True):
        nspace = "http://preservica.com/ExtendedXIP/v6.0"
        ref = str(uuid4())
        metadata = etree.SubElement(self.xip, 'Metadata', schemaUri=nspace)
        etree.SubElement(metadata, 'Ref').text = ref
        etree.SubElement(metadata, 'Entity').text = targetref
        content = etree.SubElement(metadata, 'Content')
        ex_xip = etree.SubElement(content, 'ExtendedXIP', nsmap={None: nspace})
        if surrogate:
            etree.SubElement(ex_xip, 'DigitalSurrogate').text = 'true'
        else:
            etree.SubElement(ex_xip, 'DigitalSurrogate').text = 'false'
        etree.SubElement(ex_xip, 'CoverageFrom').text = earliest
        etree.SubElement(ex_xip, 'CoverageTo').text = latest


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


def main(basedir, outdir, parent=None, security='open'):
    os.chdir(basedir)
    sip = Sip(os.path.join(outdir, os.path.split(basedir)[1]+'.zip'))
    for root, dirs, files in os.walk(os.getcwd()):
        parent = sip.add_structobj(
            os.path.split(root)[1], parent_ref=parent, security_tag=security)
        for file in files:
            fpath = pathlib.Path(root) / file
            relpath = fpath.relative_to(fpath.cwd())
            sip.add_tree(parent, relpath, security_tag=security)
    sip.write_xip()
    sip.write_protocol(parent, os.path.split(sys.argv[1])[1])
    sip.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build a simple SIP from a directory')
    parser.add_argument(
        'dir', metavar='i', type=str, help='base directory for a SIP')
    parser.add_argument(
        'out', metavar='o', type=str, help='base directory for a SIP')
    parser.add_argument(
        '--target', type=str,
        help='target folder for sip')
    parser.add_argument(
        '--security', type=str,
        help='security tag')

    args = parser.parse_args()
    main(args.dir, args.out, parent=args.target)
