import time
import pathlib
import sys
import requests
import datetime
import json
from threading import Thread
from io import BytesIO
from lxml import etree
import logging
import argparse
import subprocess
FORMAT = '%(asctime)-15s [%(levelname)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('siplog')
logger.setLevel(logging.INFO)
ENT_MAP = {
    "information-objects": "InformationObject",
    "structural-objects": "StructuralObject",
    "content-objects": "ContentObject"}
TYPE_MAP = {
    "IO": "information-objects",
    "SO": "structural-objects",
    "CO": "content-objects"}


class entity(object):
    """Class that parses out an xml response into useful attributes."""

    def __init__(self, element):
        self.xmlResponse = element
        self.type = element[0].tag.split('}')[1]
        self.XIP = element[0]
        self.ref = element.findtext(
            f'xip:{self.type}/xip:Ref', namespaces=element.nsmap)
        self.title = element.findtext(
            f'xip:{self.type}/xip:Title', namespaces=element.nsmap)
        self.securityTag = element.findtext(
            f'xip:{self.type}/xip:SecurityTag', namespaces=element.nsmap)
        self.parentRef = element.findtext(
            f'xip:{self.type}/xip:Parent', namespaces=element.nsmap)
        self.uri = element.findtext(
            'AdditionalInformation/Self', namespaces=element.nsmap)
        self.parentUri = element.findtext(
            'AdditionalInformation/Parent', namespaces=element.nsmap)
        self.parentUri = element.findtext(
            'AdditionalInformation/Self', namespaces=element.nsmap)
        self.children = element.findtext(
            'AdditionalInformation/Children', namespaces=element.nsmap)
        self.metadata = []
        for frag in element.findall('.//Metadata/Fragment', namespaces=element.nsmap):
            self.metadata.append({'schema': frag.get('schema'), 'uri': frag.text})

    def __repr__(self):
        return f'<{self.title}: {self.ref}>'


class preservica_session(requests.Session):
    """Class that handles authentication and wraps useful requests to the
    Preservica REST API. Best used as a context manager."""

    def __init__(self, login, password, host, tenant):
        super(preservica_session, self).__init__()
        logging.info("Starting session")
        self.host = host
        self.tenant = tenant
        self.headers = {
                    'Accept': "*/*",
                    'Cache-Control': "no-cache",
                    'Host': self.host,
                    'Accept-Encoding': "gzip, deflate",
                    'Content-Length': "0",
                    'Connection': "keep-alive",
                    'cache-control': "no-cache"
                    }
        self.baseurl = "https://"+self.host
        self.entityurl = self.baseurl+"/api/entity"
        self.authenturl = self.baseurl+"/api/accesstoken"
        self.get_token(login, password)

    def close(self):
        """
        Revokes the current token and cancels the refresh timer on session
        close. Make sure the session is closed explicitly, or use as context
        manager,otherwise update timer will cause session to hang indefinitely
        """
        url = self.authenturl+"/revoke"
        self.post(
            url,
            params={"access-token": self.headers['Preservica-Access-Token']})
        super(preservica_session, self).close()

    def get_token(self, login, password):
        """
        Gets an access token from Preservica and appends it to the session
        headers. Starts a timer to refresh after 10 minutes.
        """
        url = self.authenturl+"/login"
        querystring = {
            "username": login, "password": password, "tenant": self.tenant}
        logging.info("Authenticating with Preservica")
        response = self.post(url, data=querystring)
        if response.status_code == 200:
            data = response.json()
            tokenval = (data["token"])
            self.headers['Preservica-Access-Token'] = tokenval
            self.refresh_token = data["refresh-token"]
            self.refresh_timer = Thread(target=self.refresh, daemon=True)
            self.refresh_timer.start()
        else:
            logging.error(f"Unable to authenticate, received status code {response.status_code}")
            print(response.text)

    def refresh(self, interval=600):
        """Refreshes access token every interval."""
        while True:
            time.sleep(interval)
            url = self.authenturl+"/refresh"
            logging.info("Refreshing authentication token")
            response = self.post(url, data={"refreshToken": self.refresh_token})
            data = response.json()
            tokenval = (data["token"])
            self.headers['Preservica-Access-Token'] = tokenval
            self.refresh_token = data["refresh-token"]

    @staticmethod
    def find_config():
        """find a config.json file, looking in a preservica folder
        in your home directory, then the location of the calling script."""
        configpath = pathlib.Path().home() / '.preservica/config.json'
        if not configpath.exists():
            configpath = pathlib.Path(sys.argv[0]).parent / 'config.ini'
            if not configpath.exists():
                print("No config file found")
                exit()
        with configpath.open() as f:
            config = json.load(f)
        return config

    @staticmethod
    def write_config(host, tenant, login, password, profile='DEFAULT'):
        """Write or amend a config.json file with the credentials provided"""
        configpath = pathlib.Path().home() / '.preservica/config.json'
        if configpath.exists():
            with configpath.open() as f:
                config = json.load(f)
        else:
            if not configpath.parent.exists():
                configpath.parent.mkdir()
            config = {}
        config[profile] = {
            'Host': host, 'Tenant': tenant,
            'Username': login, 'Password': password}
        with configpath.open('w') as f:
            json.dump(config, f, indent=1)

    @classmethod
    def get_session(cls, profile='DEFAULT'):
        """Create a preservica session using a config file."""
        config = cls.find_config()
        host = config[profile]['Host']
        username = config[profile]['Username']
        password = config[profile]['Password']
        tenant = config[profile]['Tenant']
        sesh = cls(username, password, host, tenant)
        return sesh

    def make_uri(self, ref, type):
        url = self.entityurl+'/'+type+'/'+ref
        return url

    def get_type(self, uri):
        uri = uri.replace(self.entityurl+'/', '')
        type = uri.split('/')[0]
        return type

    def get_objectsbyid(self, identifier, type='code'):
        """
        Returns a list of entities matching the
        provided identifier.
        """
        parameters = {'type': type, 'value': identifier}
        r = self.get(
            self.entityurl+"/entities/by-identifier",
            params=parameters)
        tree = etree.parse(BytesIO(r.content))
        root = tree.getroot()
        objects = []
        for ent in root.findall('.//Entity', namespaces=root.nsmap):
            object = self.get_object(ent.text)
            objects.append(object)
        return objects

    def get_object(self, uri):
        """Returns an lxml element response for object of type with ref"""
        r = self.get(uri)
        if r.status_code == 200:
            tree = etree.parse(BytesIO(r.content))
            object = entity(tree.getroot())
            return object
        else:
            logger.error(
                f'Request for entity at {uri} failed'
                f'with status code {r.status_code}')

    def get_children(self, object):
        """Returns a list of objects.
        """
        if object.children is not None:
            response =  self.get(object.children)
            if response.status_code == 200:
                children = []
                tree = etree.parse(BytesIO(response.content))
                root = tree.getroot()
                for ent in root.findall('.//Child', root.nsmap):
                    object = self.get_object(ent.text)
                    children.append(object)
                return children
            else:
                logger.error(
                    f'Request for children of {object} failed with status '
                    f'{response.status_code}')

    def post_metadata(self, object, fragment):
        """Appends a new metadata fragment to object of type with ref."""
        url = object.uri+"/metadata"
        r = self.post(url, data=fragment)
        if r.status_code == 200:
            logging.info(f'Successfully added metadata fragment to {object}')
        else:
            logging.error(
                f'Error adding metadata to {object},'
                f' status code {r.status_code}')

    def replace_metadata(self, metauri, fragment):
        """Replaces the metadata fragment at metaurl."""
        self.headers['Content-Type'] = 'application/xml'
        r = self.put(metauri, data=fragment)
        if r.status_code == 200:
            logging.info(f'Successfully replaced metadata fragment {metauri}')
        else:
            logging.error(
                f'Error replacing metadata fragment {metauri}, '
                f'status code {r.status_code}')

    def update_xipmeta(self, object, tag, text):
        """Updates the given XIP meta tag for given object of type with ref."""
        object.XIP.find('xip:'+tag, namespaces=object.XIP.nsmap).text = text
        data = etree.tostring(object.XIP, pretty_print=True).decode()
        self.put(object.uri, data=data)

    def update_extended_xip(self, uri, earliest, latest, surrogate=True):
        """Updates or appends the extended XIP fragment for object of type with
        ref"""
        nspace = "http://preservica.com/ExtendedXIP/v6.0"
        extended_xip = etree.Element('ExtendedXIP', nsmap={None: nspace})
        etree.SubElement(
            extended_xip, 'DigitalSurrogate').text = str(surrogate).lower()
        etree.SubElement(
            extended_xip, 'CoverageFrom').text = earliest
        etree.SubElement(
            extended_xip, 'CoverageTo').text = latest
        extended_xip = etree.tostring(extended_xip, pretty_print=True).decode()
        meta = self.get_metadata(uri)
        xip_frags = [m for m in meta if m['schema'] == nspace]
        if xip_frags != []:
            meta_uri = xip_frags[0]['uri']  # we're assuming there's only one
            self.replace_metadata(meta_uri, extended_xip)
        else:
            self.post_metadata(uri, extended_xip)

    def upload(self, fpath, targeturi):
        """Uploads package to the target folder. Note if a parent is specified
        in the package XIP it will override the provided target.
        """
        self.headers['Content-Type'] = "application/octet-stream"
        fpath = pathlib.Path(fpath)
        url = self.make_uri(targeturi, 'structural-objects')+"/upload-package?filename=" + fpath.name
        start_time = time.time()
        logging.info(f"Upload of {fpath} commencing")
        try:
            with fpath.open('rb') as data:
                response_mref = self.post(url, data=data)
                duration = time.time() - start_time
                if response_mref.status_code == 200:
                    logging.info(
                        f"Upload of {fpath} complete,"
                        f" duration {duration}")
                else:
                    logging.error(
                        f"Upload of {fpath} failed with status"
                        f" {response_mref.status_code}")
                return(response_mref.text)
        except OSError as e:
            print(e)
        self.headers['Content-Type'] = 'application/xml'

    def s3upload(self, fpath, bucket):
        """Uploads package to S3 bucket with required metadata. Needs the
        AWS credentials configured abd boto3 installed. We might do this via
        Boto in the future"""
        from s3upload import S3upload
        S3upload(fpath, bucket)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Simple tasks using the Preservica API')
    parser.add_argument(
        '--profile', default='DEFAULT',
        help='loads session from config file with specified profile')
    parser.add_argument(
        '--config', nargs=4,
        metavar=('host', 'tenant', 'username', 'password'),
        help='saves or amends a credentials file')
    parser.add_argument(
        '--upload', nargs=2, metavar=('filepath', 'parentref'),
        help='uploads a package to parent ref via the API')
    args = parser.parse_args()
    if args.config is not None:
        preservica_session.write_config(*args.config, profile=args.profile)
    sesh = preservica_session.get_session(profile=args.profile)
    if args.upload is not None:
        sesh.upload(*args.upload)
