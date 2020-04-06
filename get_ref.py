import requests
import sys
import os
from lxml import etree
from io import StringIO, BytesIO

auth = ('lachlan.glanville@unimelb.edu.au', 'Y01KfzXna9')
enturl = "https://unimelb.preservica.com/api/entity/entities/by-identifier"

def get_ident(mods):
    i = mods.find('./identifier[@type="UMA"]')
    parameters = {'type': 'UMA', 'value': i.text}
    r = requests.get(
        enturl,
        params=parameters,
        auth=auth)
    tree = etree.parse(BytesIO(r.content))
    root = tree.getroot()
    for ent in root.findall('.//Entity', namespaces=root.nsmap):
        print(ent.get('title'), ent.get('ref'))
