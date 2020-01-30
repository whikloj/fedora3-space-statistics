
import datetime
import os.path
import stat
import xml.etree.ElementTree as ET
import sys


class Object(object):

    foxml_ns = "info:fedora/fedora-system:def/foxml#"
    foxml_model = "info:fedora/fedora-system:def/model#"
    foxml_view = "info:fedora/fedora-system:def/view#"

    def __init__(self, filename):
        self.filename = filename
        self.pid = None
        self.datastreams = []
        self.created_date = None
        self.last_modified = None
        self.ownerId = None
        self.label = None
        self.foxml_size = 0
        self._parse_xml()
        self.total_storage = self.foxml_size + self._calculate_ds_total()

    def _parse_xml(self):
        if os.path.exists(self.filename) and os.access(self.filename, os.R_OK):
            self.foxml_size = os.stat(self.filename)[stat.ST_SIZE]
            doc = ET.parse(self.filename)
            root = doc.getroot()
            attribs = root.attrib
            self.pid = attribs['PID']
            self.label = self.get_value_attribute(
                self.get_xpath(root, ".//{{{0}}}property[@NAME=\"{1}label\"]".format(
                    self.foxml_ns,
                    self.foxml_model
                ))
            )
            self.created_date = self.get_value_attribute(
                self.get_xpath(root, ".//{{{0}}}property[@NAME=\"{1}createdDate\"]".format(
                    self.foxml_ns,
                    self.foxml_model
                ))
            )
            if self.created_date[-1] == 'Z':
                # Strip trailing Zs
                self.created_date = self.created_date[:-1]

            self.last_modified = self.get_value_attribute(
                self.get_xpath(root, ".//{{{0}}}property[@NAME=\"{1}lastModifiedDate\"]".format(
                    self.foxml_ns,
                    self.foxml_view
                ))
            )
            self.ownerId = self.get_value_attribute(
                self.get_xpath(root, ".//{{{0}}}property[@NAME=\"{1}ownerId\"]".format(
                    self.foxml_ns,
                    self.foxml_model
                ))
            )
            datastreams = self.get_xpath(root, ".//{{{0}}}datastream".format(
                self.foxml_ns
            ))
            for datastream in datastreams:
                dsid = datastream.attrib['ID']
                ds = Datastream(self.pid, dsid)
                versions = datastream.findall(".//{{{0}}}datastreamVersion".format(
                    self.foxml_ns
                ))
                for v in versions:
                    if ds.get_label() is None:
                        ds.set_label(self.get_attribute(v, 'LABEL'))
                    if ds.get_mimetype() is None:
                        ds.set_mimetype(self.get_attribute(v, 'MIMETYPE'))
                    created = self.get_attribute(v, 'CREATED')
                    size = self.get_attribute(v, 'SIZE')
                    if created is not None and size is not None:
                        if created[-1] == 'Z':
                            created = created[:-1]
                        created_ts = self.fromisoformat(created)
                        size = int(size)
                        ds.add_version(created_ts, size)
                self.datastreams.append(ds)

    @staticmethod
    def get_attribute(element, attrib_name):
        try:
            return element.attrib[attrib_name]
        except KeyError:
            return None

    @staticmethod
    def get_xpath(doc, xpath):
        results = doc.findall(xpath)
        if results is not None and len(results) > 0:
            if len(results) == 1:
                return results[0]
            return results
        return None

    @staticmethod
    def get_value_attribute(element):
        temp = element.attrib
        return temp['VALUE']

    @staticmethod
    def fromisoformat(the_date):
        if sys.version_info >= (3, 7):
            return datetime.datetime.fromisoformat(the_date)
        else:
            dt, _, us = the_date.partition(".")
            dt = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
            us = int(us.rstrip("Z"), 10) if len(us.rstrip("Z")) > 0 else 0
            return dt + datetime.timedelta(microseconds=us)

    def _calculate_ds_total(self):
        total = 0
        for ds in self.datastreams:
            total += ds.get_total_size()
        return total

    def get_datastreams(self):
        return self.datastreams

    def get_owner(self):
        return self.ownerId

    def get_label(self):
        return self.label

    def get_created(self):
        return self.created_date

    def get_modified(self):
        return self.last_modified

    def get_pid(self):
        return self.pid

    def get_data(self):
        return {
            'pid': self.pid,
            'owner': self.ownerId,
            'created': self.created_date,
            'last_modified': self.last_modified,
            'label': self.label,
            'filename': self.filename,
            'object_size': self.foxml_size,
            'total_size': self.total_storage
        }


class Datastream(object):

    def __init__(self, pid, id):
        self.parent_pid = pid
        self.dsid = id
        self.versions = []
        self.label = None
        self.mimetype = None
        self.total_size = 0

    def set_dsid(self, id):
        self.dsid = id

    def set_mimetype(self, type):
        self.mimetype = type

    def get_mimetype(self):
        return self.mimetype

    def set_label(self, label):
        self.label = label

    def get_label(self):
        return self.label

    def add_version(self, created, size):
        self.versions.append((created, size))
        self.total_size += size

    def get_versions(self):
        return self.versions

    def get_total_size(self):
        return self.total_size

    def get_size_by_year(self):
        years = {}
        for created, size in self.versions:
            d = datetime.datetime.fromtimestamp(created)
            year = d.strftime('%Y')
            if year not in years.keys():
                years[year] = 0
            years[year] += size
        return years

    def get_data(self):
        return {
            'dsid': self.dsid,
            'parent_pid': self.parent_pid,
            'label': self.label,
            'mimetype': self.mimetype,
            'total_size': self.total_size,
        }
