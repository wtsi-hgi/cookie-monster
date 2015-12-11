from collections import defaultdict
from typing import Any

from hgicommon.mixable import Listenable

from cookiemonster.processor._models import RegistrationEvent

# Map where the key is the type of object the listener is interested is and the value is the listenable that will get
# updates of registration events
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
