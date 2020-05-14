import sys
import os
import meta_update
from lxml import etree


def main(xmlfile, outdir):
    tree = etree.parse(xmlfile)
    for record in tree.xpath('/table[@name="ecatalogue"]/tuple'):
        ident = record.find('atom[@name="EADUnitID"]').text
        print('exporting metadata for record', ident)
        root = meta_update.build_root(record)
        tree = etree.ElementTree(root)
        fname = os.path.join(outdir, ident+'.xml')
        tree.write(
            fname, pretty_print=True, standalone=True, xml_declaration=True,
            encoding='UTF-8')


main(sys.argv[1], sys.argv[2])
