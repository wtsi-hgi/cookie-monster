import unittest
from unittest.mock import MagicMock, call

from cookiemonster.processor._models import RegistrationEvent
from cookiemonster.processor.register import register, unregister, registration_event_listenable_map


class TestRegister(unittest.TestCase):
    """
    Tests for `register` and `unregister`.
    """
    def test_register(self):
        listener_1 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_1)
        listener_2 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_2)

        register(123)
        update_1 = RegistrationEvent(123, RegistrationEvent.Type.REGISTERED)
        listener_1.assert_called_once_with(update_1)
        listener_1.reset_mock()
        listener_2.assert_called_once_with(update_1)
        listener_2.reset_mock()

    def test_unregister(self):
        listener_1 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_1)
        listener_2 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_2)

        unregister(123)
        update_1 = RegistrationEvent(123, RegistrationEvent.Type.UNREGISTERED)
        listener_1.assert_called_once_with(update_1)
        listener_1.reset_mock()
        listener_2.assert_called_once_with(update_1)
        listener_2.reset_mock()

    def test_register_can_be_unsubscribed(self):
        listener_1 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_1)
        listener_2 = MagicMock()
        registration_event_listenable_map[int].add_listener(listener_2)

        register(123)
        update_1 = RegistrationEvent(123, RegistrationEvent.Type.REGISTERED)
        registration_event_listenable_map[int].remove_listener(listener_2)
        self.assertEquals(len(registration_event_listenable_map[int].get_listeners()), 1)

        register(456)
        unregister(456)

        listener_2.assert_called_once_with(update_1)

    def tearDown(self):
        listeners = registration_event_listenable_map[int].get_listeners()
        for listener in listeners:
            registration_event_listenable_map[int].remove_listener(listener)
