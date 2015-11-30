'''
Common Models
=============

Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''
from datetime import datetime
from typing import Any, Union, Optional

from hgicommon.collections import Metadata
from hgicommon.models import Model

from cookiemonster.common.collections import EnrichmentCollection
from cookiemonster.common.enums import MetadataNS, EnrichmentSource


class FileUpdate(Model):
    """
    Model of a file update.
    """
    def __init__(self, file_id: str, file_hash: hash, timestamp: datetime, metadata: Metadata):
        """
        Constructor.
        :param file_id: a unique identifier of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        :param metadata: the metadata of the file
        """
        self.file_id = file_id
        self.file_hash = file_hash
        self.timestamp = timestamp
        self.metadata = metadata


class CookieCrumbs(Metadata):
    '''
    CookieCrumbs is just an alias of the generic metadata model, with
    the notable extension that attributes are namespaced by source and
    section. For the sake of consistency and commonality, recognised
    namespaces are enumerated under MetadataNS and the `get` and `set`
    methods have namespaced equivalents with this extra parameter. In
    the underlying representation, everything is converted to a colon-
    -delimited string (i.e., `{source}:{section}:{key}`).

    Note that the original and __getitem__ and __setitem__ magic methods
    are NOT overridden, so the string representation must be used when
    interfacing in this manner (e.g., data['foo:bar:baz'] = 123, etc.)
    '''
    @staticmethod
    def _to_attribute(namespace: MetadataNS, key: str) -> str:
        return '{}:{}'.format(namespace.value, key)

    def get_by_namespace(self, namespace: MetadataNS, key: str, default=None):
        attribute = CookieCrumbs._to_attribute(namespace, key)
        super().get(attribute, default)

    def set_by_namespace(self, namespace: MetadataNS, key: str, value):
        attribute = CookieCrumbs._to_attribute(namespace, key)
        super().set(attribute, value)


class Enrichment(Model):
    '''
    Metadata enrichment model
    '''
    def __init__(self, source: Union[EnrichmentSource, str], timestamp: datetime, metadata: CookieCrumbs):
        '''
        Constructor

        @param  source     Source of metadata enrichment
        @param  timestamp  Timestamp of enrichment
        '''
        self.source    = source
        self.timestamp = timestamp
        self.metadata  = metadata


class Cookie(Model):
    '''
    A "Cookie" is a representation of a file's iteratively enriched
    metadata
    '''
    def __init__(self, path: str):
        '''
        Constructor
        @param  path  File path
        '''
        self.path = path
        self.enrichments = EnrichmentCollection()
        self.metadata = CookieCrumbs()

    def get_metadata_by_namespace(self, namespace: MetadataNS, key: str, default=None):
        '''
        Fetch the latest existing metadata by namespace and key

        @param  namespace  Namespace
        @param  key        Attribute name
        @param  default    Default value, if key doesn't exist
        '''
        # TODO
        pass


class Notification(Model):
    """
    A model of a notification that should be sent to an external process.
    """
    def __init__(self, external_process_name: str, data: Any=None):
        """
        Default constructor.
        :param external_process_name: the name of the external process that should be informed
        :param data: the data (if any) that should be given to the external process
        """
        self.external_process_name = external_process_name
        self.data = data
