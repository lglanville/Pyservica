import siplib
import argparse
import os
import pathlib
from PIL import Image
Image.MAX_IMAGE_PIXELS = None


def build_asset(sip_path, indir, target, ident):
    """Builds a SIP with a single asset linked to multiple TIF content objects
    and a compiled PDF representation.
    """
    sip = siplib.Sip(sip_path)
    c_objects = []
    images = []
    os.chdir(indir)
    asset = sip.add_infobj(ident, target)
    sip.add_identifier(asset, ident)
    for file in os.listdir(indir):
        if file.endswith('.tif'):
            c = sip.add_contobj(file, asset)
            c_objects.append(c)
            sip.add_generation(c, 'original', [file])
            hash = siplib.hash_file(file, ['SHA256', 'SHA512'])
            sip.add_bitstream(file, hash)
            i = Image.open(file)
            ratio = min(5096/i.width, 5096/i.height)
            i = i.resize((round(i.width*ratio), round(i.height*ratio)))
            images.append(i)
    sip.add_representation("Preservation-1", asset, c_objects)
    pdf_fname = ident+'.pdf'
    images[0].save(pdf_fname, resolution=200, quality=60, save_all=True, append_images=images[1:])
    c = sip.add_contobj(pdf_fname, asset)
    sip.add_representation('Access PDF', asset, [c], type='Access')
    sip.add_generation(c, '', [pdf_fname])
    checksums = siplib.hash_file(pdf_fname, ['SHA256', 'SHA512'])
    sip.add_bitstream(pdf_fname, checksums)
    os.remove(pdf_fname)
    sip.serialise(target, ident)


def iter_folders(parent_dir, outdir, target):
    """iterates through subfolders"""
    for dir in os.scandir(parent_dir):
        if dir.is_dir():
            path = pathlib.Path(dir).absolute()
            ident = '.'.join(path.parts[len(path.parts)-3:])
            i_path = path / 'TIF'
            if i_path.exists():
                sippath = pathlib.Path(outdir, ident+'.zip')
                build_asset(sippath, i_path, target, ident)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build a series of SIPs from a directory')
    parser.add_argument(
        'dir', metavar='i', type=str, help='base directory with subfolders')
    parser.add_argument(
        '--out', metavar='o', type=str, help='path for new SIPs')
    parser.add_argument(
        '--target', type=str,
        help='ref of target folder for sip in preservica')
    parser.add_argument(
        '--iter', action='store_true',
        help='iterate through subfolders')

    args = parser.parse_args()
    if args.iter:
        iter_folders(args.dir, args.out, args.target)
    else:
        path = pathlib.Path(args.dir).absolute()
        tifpath = path / 'TIF'
        ident = '.'.join(path.parts[len(path.parts)-3:])
        sipfile = pathlib.Path(args.out, ident+'.zip')
        build_asset(sipfile, tifpath, args.target, ident)
