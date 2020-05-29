"""Script for synchronising metadata between EMu and Preservica.
From an EMu XML document generated from the 'xml for preservica' report,
pings preservica for entities with a matching identifier, then crosswalks
metadata into MODS and either posts a new fragment of replacing an existing
one."""


import sys
from lxml import etree
from datetime import date
from preservicaAPI import get_session
from datefuncs import get_iso

nsmap = {
    'mods': 'http://www.loc.gov/mods/v3',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}


def add_field(parent, field, sourcenode, **kwargs):
    if hasattr(sourcenode, 'text'):
        f = etree.SubElement(
            parent, '{http://www.loc.gov/mods/v3}'+field,
            attrib=kwargs)
        f.text = sourcenode.text


def build_host(element):
    lod = element.find('atom[@name="EADLevelAttribute"]').text
    host = etree.Element(
        '{http://www.loc.gov/mods/v3}relatedItem',
        type='host', displayLabel="Part of "+lod)
    add_field(
        host, 'identifier', element.find('atom[@name="EADUnitID"]'),
        type="UMA")
    title = etree.SubElement(host, '{http://www.loc.gov/mods/v3}titleInfo')
    add_field(title, 'title', element.find('atom[@name="EADUnitTitle"]'))
    oinfo = etree.SubElement(host, '{http://www.loc.gov/mods/v3}originInfo')
    add_field(
        oinfo, 'dateCreated', element.find('atom[@name="EADUnitDate"]'))
    for crtr in element.findall('table[@name="EADOriginationRef_tab"]/tuple'):
        name = etree.SubElement(host, '{http://www.loc.gov/mods/v3}name')
        add_field(
            name, 'namePart',
            crtr.find('atom[@name="NamFullName"]'))
        role = etree.SubElement(name, '{http://www.loc.gov/mods/v3}role')
        roleterm = etree.SubElement(role, '{http://www.loc.gov/mods/v3}roleterm')
        roleterm.text = 'Creator'
    return host


def build_root(record):
    root = etree.Element('{http://www.loc.gov/mods/v3}mods', nsmap=nsmap)
    root.set('version', '3.4')
    rinfo = etree.SubElement(root, '{http://www.loc.gov/mods/v3}recordInfo')
    add_field(
        rinfo, 'recordIdentifier', record.find('atom[@name="irn"]'),
        source="EMu catalogue irn")
    cdate = etree.SubElement(rinfo, '{http://www.loc.gov/mods/v3}recordCreationDate')
    cdate.text = date.today().isoformat()
    stand = etree.SubElement(rinfo, '{http://www.loc.gov/mods/v3}descriptionStandard')
    stand.text = 'University of Melbourne Archives descriptive standards'
    add_field(
        root, 'identifier', record.find('atom[@name="EADUnitID"]'),
        type="UMA")
    title = etree.SubElement(root, '{http://www.loc.gov/mods/v3}titleInfo')
    add_field(title, 'title', record.find('atom[@name="EADUnitTitle"]'))
    oinfo = etree.SubElement(root, '{http://www.loc.gov/mods/v3}originInfo')
    add_field(
        oinfo, 'dateCreated', record.find('atom[@name="EADUnitDate"]'))
    add_field(
        root, 'abstract', record.find('atom[@name="EADScopeAndContent"]'),
        displayLabel="Scope and Content")
    for crtr in record.findall('table[@name="EADOriginationRef_tab"]/tuple'):
        name = etree.SubElement(record, '{http://www.loc.gov/mods/v3}name')
        add_field(
            name, 'namePart',
            crtr.find('atom[@name="NamFullName"]'))
        role = etree.SubElement(name, '{http://www.loc.gov/mods/v3}role')
        roleterm = etree.SubElement(role, '{http://www.loc.gov/mods/v3}roleterm')
        roleterm.text = 'Creator'
    for cont in record.findall('table[@name="contributors"]/tuple'):
        name = etree.Element('{http://www.loc.gov/mods/v3}name')
        add_field(
            name, 'namePart',
            cont.find('atom[@name="NamFullName"]'))
        role = etree.Element('{http://www.loc.gov/mods/v3}role')
        add_field(
            role, 'roleTerm',
            cont.find('atom[@name="AssRelatedPartiesRelationship"]'))
        name.append(role)
        root.append(name)
    phys = etree.SubElement(
        root, '{http://www.loc.gov/mods/v3}physicalDescription')
    for elem in record.xpath('table[@name="EADExtent_tab"]/tuple/atom'):
        add_field(phys, 'extent', elem)
    for elem in record.xpath('table[@name="EADGenreForm_tab"]/tuple/atom'):
        add_field(phys, 'form', elem)
    for elem in record.xpath('table[@name="EADPhysicalDescription_tab"]/tuple/atom'):
        add_field(phys, 'note', elem, displayLabel='Technical details')
    subjects = etree.SubElement(
        root, '{http://www.loc.gov/mods/v3}subject')
    for elem in record.xpath('table[@name="EADSubject_tab"]/tuple/atom'):
        add_field(subjects, 'topic', elem)
    for elem in record.xpath('table[@name="EADName_tab"]/tuple/atom'):
        add_field(subjects, 'name', elem)
    for elem in record.xpath('table[@name="EADGeographicName_tab"]/tuple/atom'):
        add_field(subjects, 'geographic', elem)
    for elem in record.xpath('table[@name="EADPersonalName_tab"]/tuple/atom'):
        add_field(subjects, 'name', elem)
    for elem in record.xpath('table[@name="EADCorporateName_tab"]/tuple/atom'):
        add_field(subjects, 'name', elem)
    for elem in record.xpath('table[@name="EADTitle_tab"]/tuple/atom'):
        add_field(subjects, 'titleInfo', elem)
    add_field(
        root, 'accessCondition',
        record.find('atom[@name="EADAccessRestrictions"]'),
        type="access",
        displayLabel="Conditions governing access")
    add_field(
        root, 'accessCondition',
        record.find('atom[@name="EADUseRestrictions"]'),
        type="use",
        displayLabel="Conditions governing use")
    for host in record.xpath('.//tuple[@name="AssParentObjectRef"]'):
        if host.find('atom[@name="EADLevelAttribute"]').text is not None:
            root.append(build_host(host))
    return root


def main(xmlfile, session):
    tree = etree.parse(sys.argv[1])
    for record in tree.xpath('/table[@name="ecatalogue"]/tuple'):
        ident = record.find('atom[@name="EADUnitID"]').text
        title = record.find('atom[@name="EADUnitTitle"]').text
        date = record.find('atom[@name="EADUnitDate"]').text
        # iso_dates = get_iso(date)
        print('Finding refs for identifier', ident)
        objectrefs = session.get_refs(ident)
        root = build_root(record)
        for type, uris in objectrefs.items():
            for uri in uris:
                meta = [meta for meta in session.get_metadata(uri) if meta.get('schema') == "http://www.loc.gov/mods/v3"]
                session.update_xipmeta(uri, 'Title', title)
                # if iso_dates != []:
                    # session.update_extended_xip(uri, iso_dates[0], iso_dates[-1])
                if meta == []:
                    session.post_metadata(
                        uri,
                        etree.tostring(root, pretty_print=True).decode())
                else:
                    for m in meta:
                        session.replace_metadata(
                            m['uri'],
                            etree.tostring(root, pretty_print=True).decode())
    sesh.close()

if __name__ == '__main__':
    sesh = get_session()
    main(sys.argv[1], sesh)
