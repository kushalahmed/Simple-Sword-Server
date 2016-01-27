from __future__ import absolute_import
from zipfile import ZipFile
from lxml import etree
from .spec import Namespaces

from .sss_logging import logging
ssslog = logging.getLogger(__name__)

try:
    # Py2
    basestring
except NameError:
    # Py3
    basestring = str

class DisseminationPackager(object):
    def __init__(self, dao, uri_manager):
        pass
        
    """
    Interface for all classes wishing to provide dissemination packaging services to the SSS
    """
    def package(self, collection, id):
        """
        Package up all the content in the specified container.  This method must be implemented by the extender.  The
        method should create a package in the store directory, and then return to the caller the path to that file
        so that it can be served back to the client
        """
        pass
        
    def get_uri(self):
        return "http://purl.org/net/sword/package/SimpleZip"
        
class IngestPackager(object):
    def __init__(self, dao):
        pass
        
    def ingest(self, collection, id, filename, metadata_relevant):
        """
        The package with the supplied filename has been placed in the identified container.  This should be inspected
        and unpackaged.  Implementations should note that there is optionally an atom document in the container which
        may need to be inspected, and this can be retrieved from DAO.get_atom_content().  If the metadata_relevant
        argument is False, implementations should not change the already extracted metadata in the container
        """
        return []
   
class DefaultDisseminator(DisseminationPackager):
    """
    Basic default packager, this just zips up everything except the SSS specific files in the container and stores
    them in a file called sword-default-package.zip.
    """
    def __init__(self, dao, uri_manager):
        self.dao = dao

    def package(self, collection, id):
        """ package up the content """

        # get a list of the relevant content files
        files = self.dao.list_content(collection, id, exclude=["sword-default-package.zip"])

        '''
        #FIXME: Not working opening/creating zip file. Creating remote zip file should NOT work anyway.
        #We should rather gather information about the packe for dissemination from the remote repository server.
        # create a zip file with all the original zip files in it
        zpath =  self.dao.get_store_path(collection, id, "sword-default-package.zip")
        z = ZipFile(zpath, "w")
        for file in files:
            z.write(self.dao.get_store_path(collection, id, file), file)
        z.close()

        # return the path to the package to the caller
        return zpath
        '''
        # quick fix, returned the names of the files that the resource contains.
        return files


class FeedDisseminator(DisseminationPackager):
    def __init__(self, dao, uri_manager):
        self.dao = dao
        self.ns = Namespaces()
        self.um = uri_manager
        self.nsmap = {None: self.ns.ATOM_NS}

    def package(self, collection, id):
        """ create a feed representation of the package """
        # get a list of the relevant content files
        files = self.dao.list_content(collection, id, exclude=["mediaresource.feed.xml"])

        # create a feed object with all the files as entries
        feed = etree.Element(self.ns.ATOM + "feed", nsmap=self.nsmap)
        
        for file in files:
            entry = etree.SubElement(feed, self.ns.ATOM + "entry")
            
            em = etree.SubElement(entry, self.ns.ATOM + "link")
            em.set("rel", "edit-media")
            em.set("href", self.um.part_uri(collection, id, file))
            
            edit = etree.SubElement(entry, self.ns.ATOM + "link")
            edit.set("rel", "edit")
            edit.set("href", self.um.part_uri(collection, id, file) + ".atom")
            
            content = etree.SubElement(entry, self.ns.ATOM + "link")
            content.set("type", "application/octet-stream") # FIXME: we're not storing content types, so we don't know
            content.set("src", self.um.part_uri(collection, id, file))
        
        fpath = self.dao.get_store_path(collection, id, "mediaresource.feed.xml")
        f = open(fpath, "wb")
        f.write(etree.tostring(feed, pretty_print=True))
        f.close()
        
        return fpath
        
    def get_uri(self):
        return None
        
class BinaryIngester(IngestPackager):
    def __init__(self, dao):
        pass
        
    def ingest(self, collection, id, filename, metadata_relevant):
        # does nothing, we don't try to unpack binary deposits
        return []

class SimpleZipIngester(IngestPackager):
    def __init__(self, dao):
        self.dao = dao
        self.ns = Namespaces()
        
    def ingest(self, collection, id, filename, metadata_relevant=True):
        # First, let's just extract all the contents of the zip
        # FIXME: ZipFile can't open a zip with a remote repository path.
        # It should rather fetch an xml file that stores the paths of the content files.
        #z = ZipFile(self.dao.get_store_path(collection, id, filename))
        
        # keep track of the names of the files in the zip, as these will become
        # our derived resources
        # FIXME: derived_resources will contain the list of files inside the Zip file.
        #derived_resources = z.namelist()
        # quick fix
        derived_resources = {}
        
        # FIXME: what we do here is intrinsically insecure, but SSS is not a
        # production service, so we're not worrying about it!
        path = self.dao.get_store_path(collection, id)
        # FIXME: extraction must be done by sending appropriate request to repository server
        # quick fix: doing nothing
        #z.extractall(path)

        # check for the atom document
        atom = self.dao.get_atom_content(collection, id)
        if atom is None:
            # there's no metadata to extract so just leave it
            return derived_resources

        # if the metadata is not relevant, then we don't need to continue
        if not metadata_relevant:
            return derived_resources
            
        metadata = {}
        entry = etree.fromstring(atom)

        # go through each element in the atom entry and just process the ones we care about
        # explicitly retrieve the atom based metadata first
        for element in entry.getchildren():
            if element.tag == self.ns.ATOM + "title":
                self.a_insert(metadata, "title", element.text.strip())
            if element.tag == self.ns.ATOM + "updated":
                self.a_insert(metadata, "date", element.text.strip())
            if element.tag == self.ns.ATOM + "author":
                authors = ""
                for names in element.getchildren():
                    authors += names.text.strip() + " "
                self.a_insert(metadata, "creator", authors.strip())
            if element.tag == self.ns.ATOM + "summary":
                self.a_insert(metadata, "abstract", element.text.strip())

        # now go through and retrieve the dcterms from the entry
        for element in entry.getchildren():
            if not isinstance(element.tag, basestring):
                continue
                
            # we operate an additive policy with metadata.  Duplicate
            # keys are allowed, but duplicate key/value pairs are not.
            if element.tag.startswith(self.ns.DC):
                key = element.tag[len(self.ns.DC):]
                val = element.text.strip()
                self.a_insert(metadata, key, val)

        self.dao.store_metadata(collection, id, metadata)
        
        return derived_resources
        
    def a_insert(self, d, key, value):
        if key in d:
            vs = d[key]
            if value not in vs:
                d[key].append(value)
        else:
            d[key] = [value]

class METSDSpaceIngester(IngestPackager):
    def ingest(self, collection, id, filename, metadata_relevant):
        # we don't need to implement this, it is just for example.  it would unzip the file and import the metadata
        # in the zip file
        return []

class DefaultEntryIngester(object):
    def __init__(self, dao):
        self.dao = dao
        self.ns = Namespaces()
        
    def ingest(self, collection, id, atom, additive=False):
        ssslog.debug("Ingesting Metadata; Additive? " + str(additive))
        
        # store the atom
        self.dao.store_atom(collection, id, atom)
        
        # now extract/augment the metadata
        metadata = {}
        if additive:
            # start with any existing metadata
            metadata = self.dao.get_metadata(collection, id)
        
        ssslog.debug("Existing Metadata (before new ingest): " + str(metadata))
        
        ssslog.debug("Incoming atom: " + str(atom))
        entry = etree.fromstring(atom)

        # go through each element in the atom entry and just process the ones we care about
        # explicitly retrieve the atom based metadata first
        for element in entry.getchildren():
            if element.tag == self.ns.ATOM + "title":
                self.a_insert(metadata, "title", element.text.strip())
            if element.tag == self.ns.ATOM + "updated":
                self.a_insert(metadata, "date", element.text.strip())
            if element.tag == self.ns.ATOM + "author":
                authors = ""
                for names in element.getchildren():
                    authors += names.text.strip() + " "
                self.a_insert(metadata, "creator", authors.strip())
            if element.tag == self.ns.ATOM + "summary":
                self.a_insert(metadata, "abstract", element.text.strip())

        # now go through and retrieve the dcterms from the entry
        for element in entry.getchildren():
            if not isinstance(element.tag, basestring):
                continue
                
            # we operate an additive policy with metadata.  Duplicate
            # keys are allowed, but duplicate key/value pairs are not.
            if element.tag.startswith(self.ns.DC):
                key = element.tag[len(self.ns.DC):]
                val = element.text.strip()
                self.a_insert(metadata, key, val)

        ssslog.debug("Current Metadata (extracted + previously existing): " + str(metadata))

        self.dao.store_metadata(collection, id, metadata)

    def a_insert(self, d, key, value):
        if key in d:
            vs = d[key]
            if value not in vs:
                d[key].append(value)
        else:
            d[key] = [value]
