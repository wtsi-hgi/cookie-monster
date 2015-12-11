import logging
import os
import shutil
import unittest
from tempfile import mkdtemp, mkstemp
from unittest.mock import MagicMock, call, Mock

from cookiemonster.common.models import RegistrationEvent
from cookiemonster.common.register import register, unregister, registration_event_listenable_map
from cookiemonster.tests.processor._stubs import StubRegisteringSource


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

        register(456)
        unregister(456)

        listener_2.assert_called_once_with(update_1)

    def tearDown(self):
        listeners = registration_event_listenable_map[int].get_listeners()
        for listener in listeners:
            registration_event_listenable_map[int].remove_listener(listener)


class TestRegisteringSource(unittest.TestCase):
    """
    Tests for `RegisteringSource`.
    """
    def setUp(self):
        self.temp_directory = mkdtemp(suffix=TestRegisteringSource.__name__)
        self.source = StubRegisteringSource(self.temp_directory, int)
        self.source.is_data_file = MagicMock(return_value=True)

    def test_extract_data_from_file(self):
        listener = Mock()
        registration_event_listenable_map[int].add_listener(listener)

        rule_file_location = self._create_rule_file_in_temp_directory()
        with open(rule_file_location, 'w') as file:
            file.write("from cookiemonster import register\n"
                       "register(123)\n"
                       "register(456)")

        self.source.extract_data_from_file(rule_file_location)

        listener.assert_has_calls([
            call(RegistrationEvent(123, RegistrationEvent.Type.REGISTERED)),
            call(RegistrationEvent(456, RegistrationEvent.Type.REGISTERED))
        ])

    def test_extract_data_from_file_with_corrupted_file(self):
        rule_file_location = self._create_rule_file_in_temp_directory()
        with open(rule_file_location, 'w') as file:
            file.write("~")

        logging.basicConfig(level=logging.ERROR)
        self.source.extract_data_from_file(rule_file_location)
        # Asserting no exception is raised

    def _create_rule_file_in_temp_directory(self) -> str:
        """
        Creates a rule file in the temp directory used by this test.
        :return: the file path of the created file
        """
        temp_file_location = mkstemp()[1]
        rule_file_location = "%s.py" % temp_file_location
        os.rename(temp_file_location, rule_file_location)
        return rule_file_location

    def tearDown(self):
        self.source.stop()
        shutil.rmtree(self.temp_directory)

        listenable = registration_event_listenable_map[int]
        for listener in listenable.get_listeners():
            listenable.remove_listener(listener)
