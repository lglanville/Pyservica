from lxml import etree
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
HASH_BLOCK_SIZE = 512 * 1024
SUPPORTED_ALGS = ['MD5', 'SHA1', 'SHA256', 'SHA512']


class Sip(zipfile.ZipFile):
    def __init__(self, fpath, parent, name=None):
        """Class representing a Preservica V6 Submission Information Package
        (SIP). Initialises a new, empty SIP at fpath, or if fpath exists,
        loads the SIP for modification or analysis.
        """
        if os.path.exists(fpath):
            logger.info(f'Opening existing SIP at {fpath}')
            super(Sip, self).__init__(fpath, 'a')
            for file in self.filelist:
                if file.filename.endswith('.protocol'):
                    prot = file.filename
                    self.sipref = prot.split('.')[0]
                if file.filename.endswith('metadata.xml'):
                    self.xip = etree.parse(
                        BytesIO(self.read(file.filename))).getroot()
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
            self.parent = parent
            if name is None:
                self.name = pathlib.Path(fpath).stem
            else:
                self.name = name

    def get_structs(self):
        structs = {}
        for e in self.xip.findall('StructuralObject', namespaces=self.xip.nsmap):
            title = e.findtext('Title', namespaces=self.xip.nsmap)
            ref = e.findtext('Ref', namespaces=self.xip.nsmap)
            structs[title] = ref
        return structs

    def get_infobjs(self):
        infobjs = {}
        for e in self.xip.findall('InformationObject', namespaces=self.xip.nsmap):
            title = e.findtext('Title', namespaces=self.xip.nsmap)
            ref = e.findtext('Ref', namespaces=self.xip.nsmap)
            infobjs[ref] = title
        return infobjs

    def get_checksums(self):
        """Returns a dict of file names with checksum algorithms and values."""
        sums = {}
        for elem in self.xip.findall('Bitstream', namespaces=self.xip.nsmap):
            name = elem.findtext('Filename', namespaces=self.xip.nsmap)
            sums[name] = {}
            for fixity in elem.findall('Fixities/Fixity', namespaces=self.xip.nsmap):
                alg = fixity.findtext(
                    'FixityAlgorithmRef', namespaces=self.xip.nsmap)
                hash = fixity.findtext(
                    'FixityValue', namespaces=self.xip.nsmap)
                sums[name][alg] = hash
        return sums

    def get_info(self):
        """Set filecount and filesize attributes for the protocol file."""
        self.filecount = 0
        self.filesize = 0
        dirs = []
        for entry in self.infolist():
            if entry.filename.startswith(self.content):
                self.filesize += entry.file_size
                fpath = pathlib.Path(entry.filename).relative_to(self.content)
                for x in range(len(fpath.parts)):
                    p = pathlib.Path(*fpath.parts[:x+1])
                    if p not in dirs:
                        self.filecount += 1
                        dirs.append(p)

    def get_children(self, element):
        if element.tag == "{http://preservica.com/XIP/v6.0}StructuralObject":
            c = [elem for elem in self.xip if elem.findtext('Parent', namespaces=elem.nsmap) == element.findtext('Ref', namespaces=elem.nsmap)]
            return c
        elif element.tag == "{http://preservica.com/XIP/v6.0}InformationObject":
            c = [elem for elem in self.xip if elem.findtext('InformationObject', namespaces=elem.nsmap) == element.findtext('Ref', namespaces=elem.nsmap)]
            return c
        elif element.tag == "{http://preservica.com/XIP/v6.0}Representation":
            c = [elem for elem in self.xip if element.findtext('Ref', namespaces=elem.nsmap) in [e.text for e in element.findall('ContentObjects/ContentObject', namespaces=element.nsmap)]]
            return c
        elif element.tag == "{http://preservica.com/XIP/v6.0}ContentObject":
            c = [elem for elem in self.xip if elem.findtext('ContentObject', namespaces=elem.nsmap) == element.findtext('Ref', namespaces=elem.nsmap)]
            return c
        elif element.tag == "{http://preservica.com/XIP/v6.0}Generation":
            bstreams = [elem.text for elem in element.findall('Bitstreams/Bitstream', namespaces=element.nsmap)]
            belements = []
            for elem in self.xip.findall('Bitstream', namespaces=self.xip.nsmap):
                name = element.findtext('Filename', namespaces=element.nsmap)
                loc = element.findtext('PhysicalLocation', namespaces=element.nsmap)
                fpath = pathlib.Path(loc, name).as_posix()
                if fpath in bstreams:
                    belements.append(elem)
            return belements

    def get_repr(self, element):
        ents = [
            "{http://preservica.com/XIP/v6.0}StructuralObject",
            "{http://preservica.com/XIP/v6.0}InformationObject",
            "{http://preservica.com/XIP/v6.0}ContentObject"]
        if element.tag in ents:
            tag = element.tag.split('}')[1]
            title = element.findtext('Title', namespaces=element.nsmap)
            ref = element.findtext('Ref', namespaces=element.nsmap)
            return f"{tag} {ref} {title}"
        elif element.tag == "{http://preservica.com/XIP/v6.0}Representation":
            type = element.findtext('Type', namespaces=element.nsmap)
            name = element.findtext('Name', namespaces=element.nsmap)
            return f"Representation {name}, {type}"
        elif element.tag == "{http://preservica.com/XIP/v6.0}Generation":
            label = element.findtext('Label', namespaces=element.nsmap)
            return f"Generation {label}"
        elif element.tag == "{http://preservica.com/XIP/v6.0}Bitstream":
            name = element.findtext('Filename', namespaces=element.nsmap)
            return f"Bitstream {name}"

    def get_top(self):
        """Get the elements representing the top level object(s) in the SIP."""
        top = []
        refs = [elem.findtext('Ref', namespaces=self.xip.nsmap) for elem in self.xip]
        for elem in self.xip:
            if elem.findtext('Parent', namespaces=self.xip.nsmap) not in refs:
                top.append(elem)
        return top

    def get_object(self, ref):
        for elem in self.xip.findall('.//Ref', self.xip.nsmap):
            if elem.text == ref:
                return elem.getparent()

    def list_elements(self):
        for elem in self.xip:
            print(self.get_repr(elem))

    def add_xipelement(self, root, tag, **kwargs):
        """Adds a subelement within the XIP namespace."""
        elem = etree.SubElement(
            root, etree.QName("{http://preservica.com/XIP/v6.0}"+tag), **kwargs)
        return elem

    def add_structobj(self, title, parent_ref=None, security_tag='open'):
        """Structural objects make up the hierarchy within your archive. They
        can contain other structural objects, building up a tree structure, and
        also information objects. Parent is the uuid of the destination folder
        in Preservica.
        """
        ref = str(uuid4())
        logger.info(f'Adding StructuralObject {title} {ref}')
        sobj = self.add_xipelement(
            self.xip, 'StructuralObject')
        self.add_xipelement(sobj, 'Ref').text = ref
        self.add_xipelement(sobj, 'Title').text = title
        self.add_xipelement(
            sobj, 'SecurityTag').text = security_tag
        if parent_ref is not None:
            self.add_xipelement(sobj, 'Parent').text = parent_ref
        return ref

    def add_infobj(self, title, folder_ref, security_tag='open'):
        """A logically atomic piece of information, for example a picture or an
        email. Assets are the entities which you will generally interact with,
        for example by rendering or downloading them. An asset can’t contain
        other assets although it has substructure (see below), it is
        self contained and not hierarchical.
        folder_ref is the uuid of the containing structural object.
        """
        ref = str(uuid4())
        logger.info(f'Adding InformationObject {title} {ref}')
        sobj = self.add_xipelement(
            self.xip, 'InformationObject')
        self.add_xipelement(sobj, 'Ref').text = ref
        self.add_xipelement(sobj, 'Title').text = title
        self.add_xipelement(sobj, 'SecurityTag').text = security_tag
        self.add_xipelement(sobj, 'Parent').text = folder_ref
        return ref

    def add_representation(self, name, info_ref, c_objects, type='Preservation'):
        """A way of viewing the information within an asset. The most common
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
        rep = self.add_xipelement(self.xip, 'Representation')
        logger.info(f'Adding Representation {name} to {info_ref}')
        self.add_xipelement(rep, 'InformationObject').text = info_ref
        self.add_xipelement(rep, 'Name').text = name
        self.add_xipelement(rep, 'Type').text = type
        con = self.add_xipelement(rep, 'ContentObjects')
        for c_object in c_objects:
            self.add_xipelement(con, 'ContentObject').text = c_object

    def add_contobj(self, fname, info_ref, security_tag='open'):
        """A logically atomic piece of content, for example an attachment or an
        email.
        """
        ref = str(uuid4())
        logger.info(f'Adding ContentObject {fname} {ref} to {info_ref}')
        content = self.add_xipelement(self.xip, 'ContentObject')
        self.add_xipelement(content, 'Ref').text = ref
        self.add_xipelement(content, 'Title').text = fname
        self.add_xipelement(content, 'SecurityTag').text = security_tag
        self.add_xipelement(content, 'Parent').text = info_ref
        return ref

    def add_generation(self, contobj_ref, label, bitstreams, orig='true', active='true'):
        """Content objects contain generations, as a sequence of dated views of
        the content in different formats. When content is preserved, a new
        generation will be created, with the content in a less at-risk format.
        Only the most recent generation of content will be used by default
        (e.g. for retrieving content to render).
        """
        gen = self.add_xipelement(
            self.xip, 'Generation', original=orig, active=active)
        logger.info(f'Adding Generation {label} to {contobj_ref}')
        self.add_xipelement(gen, 'ContentObject').text = contobj_ref
        self.add_xipelement(gen, 'Label').text = label
        self.add_xipelement(
            gen, 'EffectiveDate').text = datetime.now().isoformat()
        b = self.add_xipelement(gen, 'Bitstreams')
        for bitstream in bitstreams:
            fpath = pathlib.Path(bitstream)
            if fpath.is_absolute():
                raise ValueError('Bitstream paths must be relative:', fpath)
            self.add_xipelement(
                b, 'Bitstream').text = fpath.as_posix()
        self.add_xipelement(gen, 'Formats')
        self.add_xipelement(gen, 'Properties')

    def add_bitstream(self, fpath, checksums, write=True, arcname=None):
        """The physical content, as stored on a storage adapter. In almost all
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
            if arcname is None:
                arcname = self.content / fpath
            else:
                arcname = self.content / arcname
            self.write(fpath, self.content / fpath)
        bstream = self.add_xipelement(self.xip, 'Bitstream')
        path, name = os.path.split(fpath)
        posix_path = fpath.parent.as_posix()
        if posix_path == '.':
            posix_path = ''
        self.add_xipelement(bstream, 'Filename').text = name
        self.add_xipelement(
            bstream, 'FileSize').text = str(fpath.stat().st_size)
        self.add_xipelement(
            bstream, 'PhysicalLocation').text = posix_path
        fixities = self.add_xipelement(bstream, 'Fixities')
        for alg, hash in checksums.items():
            if alg not in SUPPORTED_ALGS:
                raise ValueError('Unsupported algorithm:', alg)
            fixity = self.add_xipelement(fixities, 'Fixity', nsmap=self.xip.nsmap)
            self.add_xipelement(
                fixity, 'FixityAlgorithmRef',
                nsmap=self.xip.nsmap).text = alg
            self.add_xipelement(
                fixity, 'FixityValue', nsmap=self.xip.nsmap).text = hash

    def add_asset_tree(self, parent_ref, fpath, security_tag='open', checksum=None):
        """Simple method for adding an InformationObject > ContentObject >
        Representation > Generation > Bitstream hierarchy where there's a 1:1
        relationship in the hierarchy.
        """
        fpath = pathlib.Path(fpath)
        i = self.add_infobj(fpath.stem, parent_ref, security_tag=security_tag)
        c = self.add_contobj(fpath.name, i, security_tag=security_tag)
        self.add_representation('Preservation-1', i, [c])
        self.add_generation(c, '', [fpath])
        if checksum is None:
            checksum = hash_file(fpath, ['SHA256', 'SHA512'])
        self.add_bitstream(fpath, checksum)

    def add_manifestation(self, info_ref, filepaths, type, security_tag='open', algorithms=['SHA256', 'SHA512'], rep_name=None, gen_label=''):
        """Add a manifestation to an existing information object. Filepaths
        is a list of files to support multipart assets. Paths must be relative.
        """
        CO_refs = []
        for file in filepaths:
            file = pathlib.Path(file)
            CO_ref = self.add_contobj(file.name, info_ref, security_tag=security_tag)
            CO_refs.append(CO_ref)
            self.add_generation(CO_ref, gen_label, [file])
            hash = hash_file(file, algorithms)
            self.add_bitstream(file, hash)
        if rep_name is None:  # add a name based on number of existing reps
            num_reps = 1
            for e in self.xip.findall('.//InformationObject', self.xip.nsmap):
                if e.text == info_ref and e.getparent().tag == 'Representation':
                    if e.getparent().findtext('Type', self.xip.nsmap) == type:
                        num_reps += 1
            rep_name = type+'-'+str(num_reps)
        self.add_representation(rep_name, info_ref, CO_refs)

    def sortkey(self, elem):
        return elem.findtext('Name', namespaces=elem.nsmap)

    def sort_xip(self):
        """Sorts XIP xml by entity for readability and correct
        processing by Preservica.
        """
        data = []
        data.extend(
            self.xip.findall('StructuralObject', namespaces=self.xip.nsmap))
        data.extend(
            self.xip.findall('InformationObject', namespaces=self.xip.nsmap))
        reps = sorted(
            self.xip.findall('Representation', namespaces=self.xip.nsmap),
            key=self.sortkey, reverse=True)
        data.extend(reps)
        data.extend(self.xip.findall('ContentObject', namespaces=self.xip.nsmap))
        data.extend(self.xip.findall('Generation', namespaces=self.xip.nsmap))
        data.extend(self.xip.findall('Bitstream', namespaces=self.xip.nsmap))
        the_rest = [elem for elem in self.xip if elem not in data]
        data.extend(the_rest)
        self.xip[:] = data

    def write_protocol(self):
        """
        Writes a protocol file - unsure whether this is even necessary anymore.
        """
        self.get_info()
        prot = etree.Element(
            'protocol',
            nsmap={None: "http://www.tessella.com/xipcreateprotocol/v1"})
        logger.info(f'Writing protocol')
        etree.SubElement(prot, 'dateCreated').text = datetime.now().isoformat()
        etree.SubElement(prot, 'size').text = str(self.filesize)
        etree.SubElement(prot, 'files').text = str(self.filecount)
        etree.SubElement(prot, 'submissionName').text = self.name
        etree.SubElement(prot, 'catalogueName').text = self.name
        etree.SubElement(prot, 'localAIP').text = self.sipref
        etree.SubElement(prot, 'globalAIP').text = self.parent
        etree.SubElement(prot, 'createdBy').text = getpass.getuser()
        tree = etree.ElementTree(prot)
        self.writestr(self.sipref+'.protocol', etree.tostring(
            tree, pretty_print=True, encoding="UTF-8",
            xml_declaration=True, standalone=True))

    def write_xip(self):
        logger.info(f'Writing XIP')
        self.sort_xip()
        tree = etree.ElementTree(self.xip)
        self.writestr(
            os.path.join(self.sipref, 'metadata.xml'),
            etree.tostring(
                tree, pretty_print=True, encoding="UTF-8",
                xml_declaration=True, standalone=True))

    def serialise(self):
        """
        Does all the stuff you need to do at the end.
        """
        self.write_xip()
        self.write_protocol()

    def add_identifier(self, targetref, value, type='code'):
        """
        Identifiers can be attached to StructuralObjects,
        InformationObjects or ContentObjects.
        """
        logger.info(f'Adding Identifier {type} {value} to {targetref}')
        ident = self.add_xipelement(self.xip, 'Identifier')
        self.add_xipelement(ident, 'Type').text = type
        self.add_xipelement(ident, 'Value').text = value
        self.add_xipelement(ident, 'Entity').text = targetref

    def add_metadata(self, targetref, fragment):
        """
        Metadata fragments can be attached to StructuralObjects,
        InformationObjects or ContentObjects. fragment is the root element of
        an xml tree.
        """
        ref = str(uuid4())
        nspace = fragment.tag.split('}')[0].strip('{')
        logger.info(f'Adding Metadata {nspace} to {targetref}')
        metadata = self.add_xipelement(
            self.xip, 'Metadata', schemaUri=nspace)
        self.add_xipelement(metadata, 'Ref').text = ref
        self.add_xipelement(metadata, 'Entity').text = targetref
        content = self.add_xipelement(metadata, 'Content')
        content.append(fragment)
        return ref

    def add_extendedxip(self, targetref, earliest, latest, surrogate=True):
        nspace = "http://preservica.com/ExtendedXIP/v6.0"
        ref = str(uuid4())
        logger.info(f'Adding Metadata {nspace} to {targetref}')
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


def _get_hashers(algorithms):
    hashers = {}
    for alg in algorithms:
        hashers[alg] = hashlib.new(alg)
    return hashers


def hash_file(fpath, algorithms=['SHA256', 'SHA512']):
    """
    returns a dict of hashes for the Sip.add_bitstream method.
    Supported algs are MD5, SHA1, SHA256, SHA512.
    To do: restrict to supported algorithms.
    """
    hashers = _get_hashers(algorithms)
    logger.info(f'Calculating checksums for {fpath}')
    with open(fpath, "rb") as f:
        while True:
            block = f.read(HASH_BLOCK_SIZE)
            if not block:
                break
            for i in hashers.values():
                i.update(block)
    return({alg: hasher.hexdigest() for alg, hasher in hashers.items()})


def main(basedir, outdir, parent=None, security='open', identifier=None):
    """
    Very simple method for building a V6 SIP with only single manifestations.
    """
    os.chdir(basedir)
    basedir = pathlib.Path(basedir)
    sip_path = pathlib.Path(outdir) / (basedir.name+'.zip')
    with Sip(sip_path, parent) as sip:
        for root, dirs, files in os.walk(os.getcwd()):
            parent = sip.add_structobj(
                os.path.split(root)[1],
                parent_ref=parent, security_tag=security)
            for file in files:
                fpath = pathlib.Path(root) / file
                if file == 'metadata.xml':
                    fragment = etree.parse(str(pathlib.Path(root, file)))
                    sip.add_metadata(parent, fragment.getroot())
                else:
                    relpath = fpath.relative_to(fpath.cwd())
                    sip.add_asset_tree(parent, relpath, security_tag=security)
        if identifier is not None:
            top_refs = [elem.findtext('Ref', namespaces=elem.nsmap) for elem in sip.get_top()]
            for ref in top_refs:
                sip.add_identifier(ref, identifier)
        sip.serialise()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build a simple SIP from a directory')
    parser.add_argument(
        'indir', metavar='i', type=str, help='base directory for a SIP')
    parser.add_argument(
        'out', metavar='o', type=str, help='directory for output of SIP')
    parser.add_argument(
        '--parent', type=str,
        help='parent folder ref in Preservica for SIP')
    parser.add_argument(
        '--security', type=str, default='open',
        help='security tag')
    parser.add_argument(
        '--identifier', type=str,
        help='identifier to be appended to top folder')

    args = parser.parse_args()
    main(
        args.indir,
        args.out,
        parent=args.parent,
        security=args.security,
        identifier=args.identifier)
