import logging
import os
from abc import ABCMeta
from collections import defaultdict
from importlib.util import module_from_spec, spec_from_file_location
from multiprocessing import Lock
from typing import Any, Iterable

from hgicommon.data_source import SynchronisedFilesDataSource, SourceDataType
from hgicommon.mixable import Listenable

# Map where the key is the type of object the listener is interested is and the value is the listenable that will get
# updates of registration events
from cookiemonster.common.models import RegistrationEvent

registration_event_listenable_map = defaultdict(Listenable)    # type: defaultdict[type, RegistrationEvent]


def register(registerable: Any):
    """
    Registers an object, notifying any listeners that may be interested in it.
    :param rule: the rule to register
    """
    listenable = registration_event_listenable_map[type(registerable)]
    event = RegistrationEvent(registerable, RegistrationEvent.Type.REGISTERED)
    listenable.notify_listeners(event)


def unregister(registerable: Any):
    """
    Unregisters an object, notifying any listeners that may be interested in it.
    :param rule: the rule to unregister
    """
    listenable = registration_event_listenable_map[type(registerable)]
    event = RegistrationEvent(registerable, RegistrationEvent.Type.UNREGISTERED)
    listenable.notify_listeners(event)


class RegisteringSource(SynchronisedFilesDataSource):
    """
    TODO
    """
    __metadata__ = ABCMeta

    # Global lock to allow multiple instances that source data of the same type to work
    _load_locks = defaultdict(Lock)     # type: defaultdict[type, Lock]

    def __init__(self, directory_location: str, data_type: type):
        """
        TODO
        :param directory_location:
        :param data_type:
        """
        super().__init__(directory_location)
        self._data_type = data_type

    def extract_data_from_file(self, file_path: str) -> Iterable[SourceDataType]:
        assert self.is_data_file(file_path)
        logging.info("Loading `%s` from: %s" % (self._data_type, file_path))

        if file_path.rsplit(".")[-1] != "py":
            logging.warning("Can only import uncompiled python modules that have the extension \".py\"")
            return []

        loaded = None

        def registration_event_listener(event: RegistrationEvent):
            assert event.event_type == RegistrationEvent.Type.REGISTERED
            nonlocal loaded
            loaded = event.target

        RegisteringSource._load_locks[self._data_type].acquire()
        registration_event_listenable_map[self._data_type].add_listener(registration_event_listener)

        try:
            RegisteringSource._load_module(file_path)
        except Exception as e:
            logging.warning("Failed import of \"%s\": %s" % (file_path, e))
        finally:
            registration_event_listenable_map[self._data_type].remove_listener(registration_event_listener)
            RegisteringSource._load_locks[self._data_type].release()

        if loaded is None:
            logging.warning(
                "Module \"%s\" failed to register an object of the type `%s`" % (file_path, self._data_type))
            return []
        else:
            return [loaded]

    @staticmethod
    def _load_module(path: str):
        """
        Dynamically loads the python module at the given path.
        :param path: the path to load the module from
        """
        spec = spec_from_file_location(os.path.basename(path), path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
